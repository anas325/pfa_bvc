from cdx_harvester.cdx_common import DomainConfig

CONFIG = DomainConfig(
    name="leseco",
    cdx_url_pattern="leseco.ma/*",
    date_from="20210101",
    exclude_regex=r"/(tag|category|auteur|author|page|search|feed|wp-admin|wp-content|amp)(/|$|\?)|\?page=",
)
