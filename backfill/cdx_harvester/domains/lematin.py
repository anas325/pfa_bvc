from cdx_harvester.cdx_common import DomainConfig

CONFIG = DomainConfig(
    name="lematin",
    cdx_url_pattern="lematin.ma/*",
    date_from="20210101",
    exclude_regex=r"/(tag|auteur|category|page|search|feed|wp-admin|wp-content|amp)(/|$|\?)|\?page=|/comments/",
)
