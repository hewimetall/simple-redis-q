//! Dead-letter index and one-page reads of active and scheduled tasks.
//!
//! Apalis keeps `{queue}:dead` as a sorted set scored by completion time when
//! it creates the key, and as a set when the key already is one. This module owns
//! `simple_q:dead:{queue}`, a sorted set scored by Redis `TIME` seconds.
//! Pages walk that index, or one slice of `{queue}:active` / `{queue}:scheduled`.
//! An active page is an `LRANGE` from a list offset. A consumed id does not
//! fail the next page. They do not `SMEMBERS` the dead set and they do not
//! call apalis `list_tasks`.

use anyhow::{Context, Result, bail};
use apalis_redis::Config as QueueConfig;
use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::admin::stored_job;
use crate::config::Config;
use crate::redis_api;

/// Largest page `dead` and `peek` return. Larger requests are reduced to this.
pub const PAGE_MAX: u64 = 200;

/// Longest `error` string stored on `{queue}:meta:{task_id}`.
pub const ERROR_MAX_BYTES: usize = 4096;

const NOTE_STARTED_AT: &str = r#"
local secs = tonumber(redis.call("TIME")[1])
redis.call("HSET", KEYS[1], "started_at", secs)
return secs
"#;

const RECORD_DEAD: &str = r#"
local secs = tonumber(redis.call("TIME")[1])
redis.call("HSET", KEYS[1], "reason", ARGV[1], "dead_at", secs, "deliveries", ARGV[2])
if ARGV[3] == "" then
    redis.call("HDEL", KEYS[1], "error")
else
    redis.call("HSET", KEYS[1], "error", ARGV[3])
end
redis.call("ZADD", KEYS[2], secs, ARGV[4])
return secs
"#;

/// `ARGV[1]` is how many rows to return. `ARGV[2]` is `rev` or `fwd`.
/// `ARGV[3]` and `ARGV[4]` are the exclusive cursor score and member, or empty.
const PAGE_ZSET: &str = r#"
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local rev = ARGV[2] == "rev"
local score = ARGV[3]
local member = ARGV[4]
if not limit or limit < 1 then
    return redis.error_reply("limit must be greater than zero")
end
if score == "" then
    if rev then
        return redis.call("ZREVRANGE", key, 0, limit - 1, "WITHSCORES")
    end
    return redis.call("ZRANGE", key, 0, limit - 1, "WITHSCORES")
end

local out = {}
local function push(id, sc)
    table.insert(out, id)
    table.insert(out, sc)
end
local same
if rev then
    same = redis.call("ZREVRANGEBYSCORE", key, score, score)
else
    same = redis.call("ZRANGEBYSCORE", key, score, score)
end
local start_at = nil
for index, id in ipairs(same) do
    if id == member then
        start_at = index + 1
        break
    end
end
if not start_at then
    if rev then
        for index, id in ipairs(same) do
            if id < member then
                start_at = index
                break
            end
        end
    else
        for index, id in ipairs(same) do
            if id > member then
                start_at = index
                break
            end
        end
    end
    if not start_at then
        start_at = #same + 1
    end
end
for index = start_at, #same do
    push(same[index], score)
    if #out >= limit * 2 then
        return out
    end
end
local need = limit - math.floor(#out / 2)
local rest
if rev then
    rest = redis.call(
        "ZREVRANGEBYSCORE",
        key,
        "(" .. score,
        "-inf",
        "WITHSCORES",
        "LIMIT",
        0,
        need
    )
else
    rest = redis.call(
        "ZRANGEBYSCORE",
        key,
        "(" .. score,
        "+inf",
        "WITHSCORES",
        "LIMIT",
        0,
        need
    )
end
for index = 1, #rest do
    table.insert(out, rest[index])
end
return out
"#;

const PAGE_LIST: &str = r#"
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local offset = tonumber(ARGV[2])
if not limit or limit < 1 then
    return redis.error_reply("limit must be greater than zero")
end
if not offset or offset < 0 or offset ~= math.floor(offset) then
    return redis.error_reply("cursor is not valid")
end
return redis.call("LRANGE", key, offset, offset + limit - 1)
"#;

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Sorted set of dead task ids for one queue. The score is Redis `TIME` seconds.
pub fn dead_index_key(queue: &str) -> String {
    format!("simple_q:dead:{queue}")
}

/// `0` is rejected. A value above [`PAGE_MAX`] is reduced to that cap.
pub fn bound_page_limit(limit: u64) -> Result<u64> {
    if limit == 0 {
        bail!("limit must be greater than zero");
    }
    Ok(limit.min(PAGE_MAX))
}

/// `HSET {queue}:meta:{task_id} started_at` to Redis `TIME` seconds.
///
/// Lease renewal writes `locked_at` and does not call this. The next delivery
/// overwrites `started_at`.
pub async fn note_delivery_start(
    conn: &mut ConnectionManager,
    settings: &Config,
    queue: &str,
    task_id: &str,
) -> Result<u64> {
    let config = redis_api::queue_config(queue, settings)?;
    let meta = task_meta_key(&config, task_id);
    let secs: i64 = redis::Script::new(NOTE_STARTED_AT)
        .key(&meta)
        .invoke_async(conn)
        .await
        .context("failed to record started_at")?;
    Ok(secs.max(0) as u64)
}

/// Remember why a delivery died and index the task id.
///
/// Apalis still adds the id to `{queue}:dead`. `error` is omitted when it is
/// empty, and stored text is at most [`ERROR_MAX_BYTES`] bytes. `dead_at` and
/// the index score are the same Redis `TIME` seconds.
pub async fn record_dead(
    conn: &mut ConnectionManager,
    settings: &Config,
    queue: &str,
    task_id: &str,
    reason: &str,
    error: Option<&str>,
    deliveries: u64,
) -> Result<()> {
    let config = redis_api::queue_config(queue, settings)?;
    let meta = task_meta_key(&config, task_id);
    let index = dead_index_key(queue);
    let error = cap_error(error.unwrap_or(""));
    let _: i64 = redis::Script::new(RECORD_DEAD)
        .key(&meta)
        .key(&index)
        .arg(reason)
        .arg(deliveries)
        .arg(error)
        .arg(task_id)
        .invoke_async(conn)
        .await
        .context("failed to record a dead delivery")?;
    Ok(())
}

/// One dead message, newest index entry first.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadMessage {
    pub task_id: String,
    pub message_id: String,
    pub actor_name: String,
    pub kwargs: Value,
    pub reason: String,
    pub error: Option<String>,
    pub retries: u32,
    pub deliveries: u64,
    pub dead_at: u64,
}

/// A page of dead messages. `cursor` is absent when this page is the last.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeadPage {
    pub messages: Vec<DeadMessage>,
    pub cursor: Option<String>,
}

/// One waiting task. `run_at` is set for `scheduled` and absent for `active`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuedTask {
    pub task_id: String,
    pub message_id: String,
    pub actor_name: String,
    pub kwargs: Value,
    pub retries: u32,
    pub max_retries: u32,
    pub time_limit_ms: u64,
    pub run_at: Option<u64>,
}

/// A page of active or scheduled tasks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskPage {
    pub tasks: Vec<QueuedTask>,
    pub cursor: Option<String>,
}

/// Page `simple_q:dead:{queue}` newest-first.
///
/// The cursor is exclusive. The read is `ZREVRANGE` / `ZREVRANGEBYSCORE` for
/// one page of ids, then those fields of `{queue}:data` and the per-task meta.
pub async fn dead_page(
    conn: &mut ConnectionManager,
    settings: &Config,
    queue: &str,
    limit: u64,
    cursor: Option<&str>,
) -> Result<DeadPage> {
    let limit = bound_page_limit(limit)?;
    let config = redis_api::queue_config(queue, settings)?;
    let (score, member) = dead_cursor(cursor)?;
    let rows = page_zset(
        conn,
        &dead_index_key(queue),
        limit + 1,
        true,
        &score,
        &member,
    )
    .await?;
    let (kept, next) = split_page(rows, limit, "d");
    let messages = load_dead(conn, &config, kept).await?;
    Ok(DeadPage {
        messages,
        cursor: next,
    })
}

/// Page `{queue}:active` with `LRANGE`, or `{queue}:scheduled` with `ZRANGE`.
///
/// Only that page is loaded from `{queue}:data`. `state` is `active` or
/// `scheduled`. An active cursor is a list offset. Scheduled rows include
/// `run_at`, the sorted-set score in Redis seconds.
pub async fn peek_page(
    conn: &mut ConnectionManager,
    settings: &Config,
    queue: &str,
    state: &str,
    limit: u64,
    cursor: Option<&str>,
) -> Result<TaskPage> {
    let limit = bound_page_limit(limit)?;
    let config = redis_api::queue_config(queue, settings)?;
    let (ids, next) = match state {
        "active" => {
            let offset = active_cursor(cursor)?;
            let ids = page_list(conn, &config.active_jobs_list(), limit + 1, offset).await?;
            let (kept, next) = split_ids(ids, limit, offset);
            (kept, next)
        }
        "scheduled" => {
            let (score, member) = scheduled_cursor(cursor)?;
            let rows = page_zset(
                conn,
                &config.scheduled_jobs_set(),
                limit + 1,
                false,
                &score,
                &member,
            )
            .await?;
            let (kept, next) = split_page(rows, limit, "s");
            (kept, next)
        }
        _ => bail!("state must be active or scheduled"),
    };
    let tasks = load_queued(conn, &config, &ids, state == "scheduled").await?;
    Ok(TaskPage {
        tasks,
        cursor: next,
    })
}

fn task_meta_key(config: &QueueConfig, task_id: &str) -> String {
    format!("{}:{task_id}", config.job_meta_hash())
}

async fn page_zset(
    conn: &mut ConnectionManager,
    key: &str,
    limit: u64,
    newest_first: bool,
    score: &str,
    member: &str,
) -> Result<Vec<(String, String)>> {
    let value: redis::Value = redis::Script::new(PAGE_ZSET)
        .key(key)
        .arg(limit)
        .arg(if newest_first { "rev" } else { "fwd" })
        .arg(score)
        .arg(member)
        .invoke_async(conn)
        .await
        .context("failed to page a sorted set")?;
    Ok(pairs(redis_strings(value)))
}

async fn page_list(
    conn: &mut ConnectionManager,
    key: &str,
    limit: u64,
    offset: u64,
) -> Result<Vec<String>> {
    let value: redis::Value = redis::Script::new(PAGE_LIST)
        .key(key)
        .arg(limit)
        .arg(offset)
        .invoke_async(conn)
        .await
        .context("failed to page the active list")?;
    Ok(redis_strings(value))
}

fn split_page(
    rows: Vec<(String, String)>,
    limit: u64,
    kind: &str,
) -> (Vec<(String, String)>, Option<String>) {
    let more = rows.len() as u64 > limit;
    let kept: Vec<(String, String)> = rows.into_iter().take(limit as usize).collect();
    let cursor = if more {
        kept.last()
            .map(|(member, score)| encode_score_cursor(kind, score, member))
    } else {
        None
    };
    (kept, cursor)
}

fn split_ids(ids: Vec<String>, limit: u64, offset: u64) -> (Vec<(String, String)>, Option<String>) {
    let more = ids.len() as u64 > limit;
    let kept: Vec<String> = ids.into_iter().take(limit as usize).collect();
    let cursor = if more {
        Some(encode_active_cursor(offset.saturating_add(limit)))
    } else {
        None
    };
    let rows = kept.into_iter().map(|id| (id, String::new())).collect();
    (rows, cursor)
}

/// Keep a prefix of at most [`ERROR_MAX_BYTES`] bytes, on a char boundary.
fn cap_error(text: &str) -> &str {
    if text.len() <= ERROR_MAX_BYTES {
        return text;
    }
    let mut end = ERROR_MAX_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

async fn load_dead(
    conn: &mut ConnectionManager,
    config: &QueueConfig,
    rows: Vec<(String, String)>,
) -> Result<Vec<DeadMessage>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    let payloads = hash_fields(conn, &config.job_data_hash(), &ids).await?;
    let mut pipe = redis::pipe();
    for id in &ids {
        pipe.cmd("HMGET")
            .arg(task_meta_key(config, id))
            .arg("reason")
            .arg("error")
            .arg("deliveries")
            .arg("dead_at");
    }
    let metas: Vec<redis::Value> = pipe
        .query_async(conn)
        .await
        .context("failed to read dead-letter metadata")?;
    if metas.len() != rows.len() {
        bail!("dead-letter metadata returned {} replies", metas.len());
    }
    let mut messages = Vec::with_capacity(rows.len());
    for (index, (task_id, score)) in rows.into_iter().enumerate() {
        let (message_id, actor_name, kwargs, retries, _, _) = job_fields(payloads.get(index));
        let fields = redis_slots(metas[index].clone());
        let reason = fields
            .first()
            .and_then(|text| text.clone())
            .unwrap_or_default();
        let error = fields
            .get(1)
            .and_then(|text| text.clone())
            .filter(|text| !text.is_empty());
        let deliveries = fields
            .get(2)
            .and_then(|text| text.as_deref())
            .and_then(parse_u64)
            .unwrap_or(0);
        let dead_at = fields
            .get(3)
            .and_then(|text| text.as_deref())
            .and_then(parse_u64)
            .or_else(|| parse_u64(&score))
            .unwrap_or(0);
        messages.push(DeadMessage {
            task_id,
            message_id,
            actor_name,
            kwargs,
            reason,
            error,
            retries,
            deliveries,
            dead_at,
        });
    }
    Ok(messages)
}

async fn load_queued(
    conn: &mut ConnectionManager,
    config: &QueueConfig,
    rows: &[(String, String)],
    scheduled: bool,
) -> Result<Vec<QueuedTask>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    let payloads = hash_fields(conn, &config.job_data_hash(), &ids).await?;
    let mut tasks = Vec::with_capacity(rows.len());
    for (index, (task_id, score)) in rows.iter().enumerate() {
        let (message_id, actor_name, kwargs, retries, max_retries, time_limit_ms) =
            job_fields(payloads.get(index));
        let run_at = if scheduled { parse_u64(score) } else { None };
        tasks.push(QueuedTask {
            task_id: task_id.clone(),
            message_id,
            actor_name,
            kwargs,
            retries,
            max_retries,
            time_limit_ms,
            run_at,
        });
    }
    Ok(tasks)
}

async fn hash_fields(
    conn: &mut ConnectionManager,
    key: &str,
    ids: &[&str],
) -> Result<Vec<Option<String>>> {
    let mut cmd = redis::cmd("HMGET");
    cmd.arg(key);
    for id in ids {
        cmd.arg(*id);
    }
    let values: Vec<redis::Value> = cmd
        .query_async(conn)
        .await
        .context("failed to read task payloads")?;
    if values.len() != ids.len() {
        bail!("task payloads returned {} replies", values.len());
    }
    Ok(values.into_iter().map(redis_string).collect())
}

fn job_fields(payload: Option<&Option<String>>) -> (String, String, Value, u32, u32, u64) {
    let Some(Some(payload)) = payload else {
        return empty_job();
    };
    let Some(job) = stored_job(payload) else {
        return empty_job();
    };
    let kwargs = if job.kwargs.is_object() {
        job.kwargs
    } else {
        serde_json::json!({})
    };
    (
        job.message_id,
        job.actor_name,
        kwargs,
        job.retries,
        job.max_retries,
        job.time_limit_ms,
    )
}

fn empty_job() -> (String, String, Value, u32, u32, u64) {
    (String::new(), String::new(), serde_json::json!({}), 0, 0, 0)
}

fn dead_cursor(cursor: Option<&str>) -> Result<(String, String)> {
    match decode_cursor(cursor)? {
        None => Ok((String::new(), String::new())),
        Some(Decoded::Score {
            kind,
            score,
            member,
        }) if kind == "d" => Ok((score, member)),
        Some(_) => bail!("cursor is not valid"),
    }
}

fn scheduled_cursor(cursor: Option<&str>) -> Result<(String, String)> {
    match decode_cursor(cursor)? {
        None => Ok((String::new(), String::new())),
        Some(Decoded::Score {
            kind,
            score,
            member,
        }) if kind == "s" => Ok((score, member)),
        Some(_) => bail!("cursor is not valid"),
    }
}

fn active_cursor(cursor: Option<&str>) -> Result<u64> {
    match decode_cursor(cursor)? {
        None => Ok(0),
        Some(Decoded::Active(offset)) => offset.parse().context("cursor is not valid"),
        Some(_) => bail!("cursor is not valid"),
    }
}

#[derive(Debug)]
enum Decoded {
    Active(String),
    Score {
        kind: String,
        score: String,
        member: String,
    },
}

fn decode_cursor(cursor: Option<&str>) -> Result<Option<Decoded>> {
    let Some(cursor) = cursor.map(str::trim).filter(|text| !text.is_empty()) else {
        return Ok(None);
    };
    let bytes = b64url_decode(cursor).context("cursor is not valid")?;
    let text = String::from_utf8(bytes).context("cursor is not valid")?;
    let Some((kind, rest)) = text.split_once('\n') else {
        bail!("cursor is not valid");
    };
    match kind {
        "a" => {
            if rest.is_empty()
                || rest.contains('\n')
                || !rest.bytes().all(|byte| byte.is_ascii_digit())
            {
                bail!("cursor is not valid");
            }
            Ok(Some(Decoded::Active(rest.to_owned())))
        }
        "d" | "s" => {
            let Some((score, member)) = rest.split_once('\n') else {
                bail!("cursor is not valid");
            };
            if score.is_empty()
                || member.is_empty()
                || member.contains('\n')
                || !score.bytes().all(|byte| byte.is_ascii_digit())
            {
                bail!("cursor is not valid");
            }
            Ok(Some(Decoded::Score {
                kind: kind.to_owned(),
                score: score.to_owned(),
                member: member.to_owned(),
            }))
        }
        _ => bail!("cursor is not valid"),
    }
}

fn encode_score_cursor(kind: &str, score: &str, member: &str) -> String {
    let score = parse_u64(score)
        .map(|value| value.to_string())
        .unwrap_or_else(|| score.to_owned());
    b64url_encode(format!("{kind}\n{score}\n{member}").as_bytes())
}

fn encode_active_cursor(offset: u64) -> String {
    b64url_encode(format!("a\n{offset}").as_bytes())
}

fn b64url_encode(bytes: &[u8]) -> String {
    let mut out = String::new();
    let mut index = 0;
    while index + 3 <= bytes.len() {
        let chunk = ((bytes[index] as u32) << 16)
            | ((bytes[index + 1] as u32) << 8)
            | (bytes[index + 2] as u32);
        push_b64(&mut out, (chunk >> 18) & 63);
        push_b64(&mut out, (chunk >> 12) & 63);
        push_b64(&mut out, (chunk >> 6) & 63);
        push_b64(&mut out, chunk & 63);
        index += 3;
    }
    let rest = bytes.len() - index;
    if rest == 1 {
        let chunk = (bytes[index] as u32) << 16;
        push_b64(&mut out, (chunk >> 18) & 63);
        push_b64(&mut out, (chunk >> 12) & 63);
    } else if rest == 2 {
        let chunk = ((bytes[index] as u32) << 16) | ((bytes[index + 1] as u32) << 8);
        push_b64(&mut out, (chunk >> 18) & 63);
        push_b64(&mut out, (chunk >> 12) & 63);
        push_b64(&mut out, (chunk >> 6) & 63);
    }
    out
}

fn push_b64(out: &mut String, value: u32) {
    out.push(B64[value as usize] as char);
}

fn b64url_decode(token: &str) -> Result<Vec<u8>> {
    if token.is_empty() {
        bail!("cursor is not valid");
    }
    let mut bytes = Vec::new();
    let mut buf: u32 = 0;
    let mut bits: i32 = 0;
    for byte in token.bytes() {
        let Some(value) = B64.iter().position(|item| *item == byte) else {
            bail!("cursor is not valid");
        };
        buf = (buf << 6) | value as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            bytes.push((buf >> bits) as u8);
            if bits == 0 {
                buf = 0;
            } else {
                buf &= (1_u32 << bits) - 1;
            }
        }
    }
    if bits > 0 && buf != 0 {
        bail!("cursor is not valid");
    }
    Ok(bytes)
}

fn pairs(flat: Vec<String>) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    let mut items = flat.into_iter();
    while let Some(member) = items.next() {
        let Some(score) = items.next() else {
            break;
        };
        rows.push((member, score));
    }
    rows
}

fn redis_strings(value: redis::Value) -> Vec<String> {
    redis_slots(value).into_iter().flatten().collect()
}

fn redis_slots(value: redis::Value) -> Vec<Option<String>> {
    match value {
        redis::Value::Array(items) | redis::Value::Set(items) => {
            items.into_iter().map(redis_string).collect()
        }
        redis::Value::Nil => Vec::new(),
        other => vec![redis_string(other)],
    }
}

fn redis_string(value: redis::Value) -> Option<String> {
    match value {
        redis::Value::Nil => None,
        redis::Value::BulkString(bytes) => String::from_utf8(bytes).ok(),
        redis::Value::SimpleString(text) | redis::Value::VerbatimString { text, .. } => Some(text),
        redis::Value::Int(number) => Some(number.to_string()),
        redis::Value::Double(number) if number.is_finite() => {
            if number.fract() == 0.0 {
                Some(format!("{}", number as i64))
            } else {
                Some(number.to_string())
            }
        }
        redis::Value::Double(_) => None,
        _ => None,
    }
}

fn parse_u64(text: &str) -> Option<u64> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(value) = text.parse::<u64>() {
        return Some(value);
    }
    let value = text.parse::<f64>().ok()?;
    if value.is_finite() && value >= 0.0 {
        Some(value.round() as u64)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_dead_index_is_owned_by_this_repo_and_the_page_cap_is_200() {
        assert_eq!(dead_index_key("default"), "simple_q:dead:default");
        assert_eq!(PAGE_MAX, 200);
        assert!(bound_page_limit(0).is_err());
        assert_eq!(bound_page_limit(1).unwrap(), 1);
        assert_eq!(bound_page_limit(50).unwrap(), 50);
        assert_eq!(bound_page_limit(200).unwrap(), 200);
        assert_eq!(bound_page_limit(201).unwrap(), 200);
        assert_eq!(bound_page_limit(10_000).unwrap(), 200);
    }

    #[test]
    fn cursors_round_trip_and_reject_a_different_kind() {
        let dead = encode_score_cursor("d", "1700000000", "01HZTASK");
        let scheduled = encode_score_cursor("s", "1700000001.0", "01HZLATER");
        let active = encode_active_cursor(2);
        let task_id = encode_active_cursor_bytes(b"a\n01HZACTIVE");
        match decode_cursor(Some(&dead)).unwrap() {
            Some(Decoded::Score {
                kind,
                score,
                member,
            }) => {
                assert_eq!(kind, "d");
                assert_eq!(score, "1700000000");
                assert_eq!(member, "01HZTASK");
            }
            other => panic!("dead cursor decoded as {other:?}"),
        }
        match decode_cursor(Some(&scheduled)).unwrap() {
            Some(Decoded::Score {
                kind,
                score,
                member,
            }) => {
                assert_eq!(kind, "s");
                assert_eq!(score, "1700000001");
                assert_eq!(member, "01HZLATER");
            }
            other => panic!("scheduled cursor decoded as {other:?}"),
        }
        assert!(matches!(
            decode_cursor(Some(&active)).unwrap(),
            Some(Decoded::Active(offset)) if offset == "2"
        ));
        assert_eq!(active_cursor(Some(&active)).unwrap(), 2);
        assert_eq!(active_cursor(None).unwrap(), 0);
        assert!(decode_cursor(Some(&task_id)).is_err());
        assert!(active_cursor(Some(&task_id)).is_err());
        assert!(decode_cursor(None).unwrap().is_none());
        assert!(decode_cursor(Some("")).unwrap().is_none());
        assert!(decode_cursor(Some("%%%")).is_err());
        assert!(dead_cursor(Some(&active)).is_err());
        assert!(scheduled_cursor(Some(&dead)).is_err());
        assert!(active_cursor(Some(&scheduled)).is_err());
        assert_eq!(dead_cursor(None).unwrap(), (String::new(), String::new()));
    }

    #[test]
    fn stored_error_text_stops_at_4_kib() {
        assert_eq!(cap_error("RuntimeError: boom"), "RuntimeError: boom");
        let long = "x".repeat(8_000);
        let capped = cap_error(&long);
        assert_eq!(capped.len(), ERROR_MAX_BYTES);
        assert!(long.starts_with(capped));
        let mut split = "y".repeat(ERROR_MAX_BYTES - 1);
        split.push('я');
        let capped = cap_error(&split);
        assert!(capped.len() < ERROR_MAX_BYTES);
        assert!(split.starts_with(capped));
        assert!(capped.is_char_boundary(capped.len()));
    }

    fn encode_active_cursor_bytes(bytes: &[u8]) -> String {
        b64url_encode(bytes)
    }
}
