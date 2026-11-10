//! Server side. Delete finished Redis records after their TTL, and drop
//! worker metadata hashes that no longer belong to a live worker.

use std::collections::BTreeSet;

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepStats {
    pub done: u64,
    pub dead: u64,
}

pub fn should_delete(done_at: i64, now: i64, ttl_secs: u64) -> bool {
    done_at <= now.saturating_sub(ttl_secs as i64)
}

/// Delete expired done and dead records in SSCAN batches, then drop worker
/// metadata hashes whose names are no longer in `{queue}:workers`.
///
/// Apalis `vacuum` reads a data hash with `HGETALL` and does not remove ids from
/// the done and dead sets. This walk uses a cursor and removes the payload,
/// the per-task meta hash, and the set member together.
pub async fn sweep_queue(
    conn: &mut ConnectionManager,
    queue: &str,
    now: i64,
    done_ttl_secs: u64,
    dead_ttl_secs: u64,
    batch: usize,
) -> Result<SweepStats> {
    let done = sweep_set(conn, queue, "done", now, done_ttl_secs, batch).await?;
    let dead = sweep_set(conn, queue, "dead", now, dead_ttl_secs, batch).await?;
    sweep_orphan_metadata(conn, queue, batch).await?;
    Ok(SweepStats { done, dead })
}

/// Drop `{queue}:workers:metadata:{worker_name}` after an orderly exit.
///
/// Apalis writes this hash with no TTL. Orphan requeue removes the name from
/// the workers set and leaves the hash. The caller logs a failed delete.
pub async fn delete_worker_metadata(
    conn: &mut ConnectionManager,
    queue: &str,
    worker_name: &str,
) -> Result<()> {
    let key = worker_metadata_key(queue, worker_name);
    let _: i64 = redis::cmd("DEL")
        .arg(&key)
        .query_async(conn)
        .await
        .context("failed to delete worker metadata")?;
    Ok(())
}

fn worker_metadata_key(queue: &str, worker_name: &str) -> String {
    format!("{queue}:workers:metadata:{worker_name}")
}

/// A metadata hash is deleted when its worker name is absent from the workers set.
fn should_delete_worker_metadata(worker_name: &str, members: &BTreeSet<&str>) -> bool {
    !members.contains(worker_name)
}

async fn sweep_orphan_metadata(
    conn: &mut ConnectionManager,
    queue: &str,
    batch: usize,
) -> Result<()> {
    // The board worker list is upstream.
    let prefix = worker_metadata_key(queue, "");
    let pattern = format!("{prefix}*");
    let workers = format!("{queue}:workers");
    let mut cursor = 0u64;
    loop {
        let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&pattern)
            .arg("COUNT")
            .arg(batch.max(1))
            .query_async(conn)
            .await
            .context("failed to scan worker metadata")?;
        for key in keys {
            let Some(name) = key.strip_prefix(&prefix).filter(|name| !name.is_empty()) else {
                continue;
            };
            let score: Option<String> = redis::cmd("ZSCORE")
                .arg(&workers)
                .arg(name)
                .query_async(conn)
                .await
                .context("failed to score a worker")?;
            // ZSCORE missing means the name is not a member. A score keeps the hash.
            let members = if score.is_some() {
                BTreeSet::from([name])
            } else {
                BTreeSet::new()
            };
            if should_delete_worker_metadata(name, &members) {
                let _: i64 = redis::cmd("DEL")
                    .arg(&key)
                    .query_async(conn)
                    .await
                    .with_context(|| format!("failed to delete orphan worker metadata {key}"))?;
            }
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(())
}

async fn sweep_set(
    conn: &mut ConnectionManager,
    queue: &str,
    which: &str,
    now: i64,
    ttl_secs: u64,
    batch: usize,
) -> Result<u64> {
    let set = format!("{queue}:{which}");
    let data = format!("{queue}:data");
    let mut cursor = 0u64;
    let mut deleted = 0u64;
    loop {
        let (next, ids): (u64, Vec<String>) = redis::cmd("SSCAN")
            .arg(&set)
            .arg(cursor)
            .arg("COUNT")
            .arg(batch.max(1))
            .query_async(conn)
            .await?;
        for id in ids {
            let meta = format!("{queue}:meta:{id}");
            let done_at: Option<String> = redis::cmd("HGET")
                .arg(&meta)
                .arg("done_at")
                .query_async(conn)
                .await?;
            let Some(done_at) = done_at.and_then(|value| value.parse::<i64>().ok()) else {
                continue;
            };
            if !should_delete(done_at, now, ttl_secs) {
                continue;
            }
            let _: () = redis::pipe()
                .cmd("HDEL")
                .arg(&data)
                .arg(&id)
                .cmd("DEL")
                .arg(&meta)
                .cmd("SREM")
                .arg(&set)
                .arg(&id)
                .ignore()
                .query_async(conn)
                .await?;
            deleted += 1;
        }
        cursor = next;
        if cursor == 0 {
            break;
        }
    }
    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn done_records_expire_on_their_ttl_and_fresh_dead_records_stay() {
        let now = 1_700_000_000i64;
        let day = 24 * 60 * 60u64;
        let week = 7 * day;
        assert!(should_delete(now - day as i64, now, day));
        assert!(!should_delete(now - day as i64 + 1, now, day));
        assert!(!should_delete(now - day as i64, now, week));
        assert!(should_delete(now - week as i64, now, week));
    }

    #[test]
    fn metadata_absent_from_the_member_set_is_deleted_and_a_present_member_is_kept() {
        let members = BTreeSet::from(["agent-worker.default.host.1.live"]);
        assert!(should_delete_worker_metadata(
            "agent-worker.default.host.1.gone",
            &members
        ));
        assert!(!should_delete_worker_metadata(
            "agent-worker.default.host.1.live",
            &members
        ));
    }
}
