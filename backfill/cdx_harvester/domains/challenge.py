from cdx_harvester.cdx_common import DomainConfig

CONFIG = DomainConfig(
    name="challenge",
    cdx_url_pattern="challenge.ma/*",
    date_from="20210101",
    exclude_regex=r"/(tag|category|auteur|author|page|search|feed|wp-admin|wp-content|amp)(/|$|\?)|\?page=",
)
