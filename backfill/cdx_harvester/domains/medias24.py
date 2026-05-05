from cdx_harvester.cdx_common import DomainConfig

CONFIG = DomainConfig(
    name="medias24",
    cdx_url_pattern="medias24.com/*",
    date_from="20210101",
    exclude_regex=r"/(tag|category|author|page|search|feed|wp-admin|wp-content|amp)(/|$|\?)|\?page=",
)
