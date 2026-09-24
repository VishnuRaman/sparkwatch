//! Executor logs over HTTP (YARN NodeManager / standalone worker pages).
//!
//! The `executorLogs` URLs Spark reports point at HTML pages with the log
//! inside a `<pre>`. We take that, strip tags, and treat it as a tail.

/// The log text from a NodeManager / worker log page. Plain-text responses
/// pass through untouched.
pub fn extract_log_text(body: &str) -> String {
    let lower = body.to_lowercase();
    if !lower.contains("<html") && !lower.contains("<pre") {
        return body.to_string();
    }
    // Prefer the (last) <pre> block: the NodeManager page puts navigation
    // before it, the standalone worker page a header.
    let inner = match (lower.rfind("<pre"), lower.rfind("</pre>")) {
        (Some(s), Some(e)) if e > s => {
            let open_end = body[s..].find('>').map(|i| s + i + 1).unwrap_or(s);
            &body[open_end..e]
        }
        _ => body,
    };
    unescape(&strip_tags(inner))
}

fn strip_tags(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' if in_tag => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    out
}

fn unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
}

/// YARN's NodeManager accepts `?start=-N` (bytes from the end); ask for a
/// decent tail rather than the 4 KiB Spark's link defaults to.
pub const TAIL_BYTES: i64 = 262_144;

pub fn with_tail(url: &str) -> String {
    if let Some(i) = url.find("start=") {
        let rest = &url[i + 6..];
        let end = rest.find('&').map(|e| i + 6 + e).unwrap_or(url.len());
        format!("{}start=-{TAIL_BYTES}{}", &url[..i], &url[end..])
    } else if url.contains('?') {
        format!("{url}&start=-{TAIL_BYTES}")
    } else {
        format!("{url}?start=-{TAIL_BYTES}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_pre_block_and_unescapes() {
        let page = "<html><body><h1>Logs for container_1</h1>\n<pre>26/09/24 INFO Executor: a &lt;b&gt;\n26/09/24 ERROR x &amp; y</pre></body></html>";
        assert_eq!(
            extract_log_text(page),
            "26/09/24 INFO Executor: a <b>\n26/09/24 ERROR x & y"
        );
        assert_eq!(extract_log_text("plain\ntext"), "plain\ntext");
    }

    #[test]
    fn tail_parameter_is_replaced_or_added() {
        assert_eq!(
            with_tail("http://nm:8042/node/containerlogs/c1/user/stderr?start=-4096"),
            format!("http://nm:8042/node/containerlogs/c1/user/stderr?start=-{TAIL_BYTES}")
        );
        assert_eq!(
            with_tail("http://w:8081/logPage?appId=a&executorId=1&logType=stderr"),
            format!("http://w:8081/logPage?appId=a&executorId=1&logType=stderr&start=-{TAIL_BYTES}")
        );
    }
}
