use std::sync::OnceLock;

use regex::Regex;

pub const TELEGRAM_TEXT_CAP: usize = 3_800;

pub fn capped_plain_text(input: &str) -> String {
    input.chars().take(TELEGRAM_TEXT_CAP).collect()
}

pub fn telegram_html(input: &str) -> String {
    markdown_to_telegram_html(&capped_plain_text(input))
}

fn markdown_to_telegram_html(input: &str) -> String {
    let mut stash = Vec::new();

    let text = fence_re()
        .replace_all(input, |captures: &regex::Captures<'_>| {
            keep(
                &mut stash,
                format!(
                    "<pre>{}</pre>",
                    escape_html(captures.get(1).unwrap().as_str())
                ),
            )
        })
        .into_owned();
    let text = inline_code_re()
        .replace_all(&text, |captures: &regex::Captures<'_>| {
            keep(
                &mut stash,
                format!(
                    "<code>{}</code>",
                    escape_html(captures.get(1).unwrap().as_str())
                ),
            )
        })
        .into_owned();

    let mut text = escape_html(&text);
    text = link_re()
        .replace_all(&text, |captures: &regex::Captures<'_>| {
            let href = escape_href_quotes(captures.get(2).unwrap().as_str());
            format!(
                "<a href=\"{}\">{}</a>",
                href,
                captures.get(1).unwrap().as_str()
            )
        })
        .into_owned();
    text = bold_re().replace_all(&text, "<b>$1</b>").into_owned();
    text = italicize(&text);
    text = heading_re().replace_all(&text, "<b>$1</b>").into_owned();
    text = bullet_re().replace_all(&text, "$1• ").into_owned();
    placeholder_re()
        .replace_all(&text, |captures: &regex::Captures<'_>| {
            let index = captures.get(1).unwrap().as_str().parse::<usize>().unwrap();
            stash[index].clone()
        })
        .into_owned()
}

fn keep(stash: &mut Vec<String>, value: String) -> String {
    stash.push(value);
    format!("\0{}\0", stash.len() - 1)
}

fn escape_html(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn escape_href_quotes(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#x27;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn italicize(input: &str) -> String {
    italic_re()
        .replace_all(input, |captures: &regex::Captures<'_>| {
            let prefix = captures.get(1).map_or("", |m| m.as_str());
            let body = captures.get(2).unwrap().as_str();
            let suffix = captures.get(3).map_or("", |m| m.as_str());
            format!("{prefix}<i>{body}</i>{suffix}")
        })
        .into_owned()
}

fn fence_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)```[\w+-]*\n?(.*?)```").unwrap())
}

fn inline_code_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"`([^`\n]+)`").unwrap())
}

fn link_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"\[([^\]]+)\]\((https?://[^)\s]+)\)"#).unwrap())
}

fn bold_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?s)\*\*(.+?)\*\*").unwrap())
}

fn italic_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(^|[^*\w])\*([^*\n]+?)\*([^\w]|$)").unwrap())
}

fn heading_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?m)^\s{0,3}#{1,6}\s+(.+?)\s*$").unwrap())
}

fn bullet_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?m)^(\s*)[-*]\s+").unwrap())
}

fn placeholder_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new("\0(\\d+)\0").unwrap())
}

#[cfg(test)]
mod tests {
    use super::{capped_plain_text, telegram_html, TELEGRAM_TEXT_CAP};

    #[test]
    fn markdown_renderer_escapes_html_and_preserves_snake_case() {
        assert_eq!(
            telegram_html("**ok** <x> snake_case"),
            "<b>ok</b> &lt;x&gt; snake_case"
        );
    }

    #[test]
    fn markdown_renderer_supports_headings_bullets_code_italic_and_links() {
        assert_eq!(
            telegram_html("# Title\n- *item* `code`\n```rs\n<x>\n```\n[site](https://example.com)"),
            "<b>Title</b>\n• <i>item</i> <code>code</code>\n<pre>&lt;x&gt;\n</pre>\n<a href=\"https://example.com\">site</a>"
        );
    }

    #[test]
    fn markdown_renderer_escapes_quotes_in_link_urls() {
        assert_eq!(
            telegram_html(
                "[site](https://example.com/path?double=\"quoted\"&single='quoted')"
            ),
            "<a href=\"https://example.com/path?double=&quot;quoted&quot;&amp;single=&#x27;quoted&#x27;\">site</a>"
        );
    }

    #[test]
    fn plain_text_helpers_cap_source_before_html_expansion() {
        let input = "&".repeat(TELEGRAM_TEXT_CAP + 50);
        assert_eq!(capped_plain_text(&input).len(), TELEGRAM_TEXT_CAP);
        assert_eq!(telegram_html(&input), "&amp;".repeat(TELEGRAM_TEXT_CAP));
    }
}
