//! Server side. Delete finished Redis records after their TTL.

use anyhow::Result;
use redis::aio::ConnectionManager;

use crate::redis_api::connection;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepStats {
    pub done: u64,
    pub dead: u64,
}

pub fn should_delete(done_at: i64, now: i64, ttl_secs: u64) -> bool {
    done_at <= now.saturating_sub(ttl_secs as i64)
}

/// Delete expired done and dead records in SSCAN / HSCAN batches.
///
/// Apalis `vacuum` reads a data hash with `HGETALL` and does not remove ids from
/// the done and dead sets. This walk uses a cursor and removes the payload,
/// the per-task meta hash, and the set member together.
pub async fn sweep_queue(
    queue: &str,
    now: i64,
    done_ttl_secs: u64,
    dead_ttl_secs: u64,
    batch: usize,
) -> Result<SweepStats> {
    let mut conn = connection()?;
    let done = sweep_set(&mut conn, queue, "done", now, done_ttl_secs, batch).await?;
    let dead = sweep_set(&mut conn, queue, "dead", now, dead_ttl_secs, batch).await?;
    Ok(SweepStats { done, dead })
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
        // One data-hash page per set page, so a large hash is never read at once.
        let _: (u64, Vec<String>) = redis::cmd("HSCAN")
            .arg(&data)
            .arg(0)
            .arg("COUNT")
            .arg(batch.max(1))
            .query_async(conn)
            .await
            .unwrap_or((0, Vec::new()));
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
}
