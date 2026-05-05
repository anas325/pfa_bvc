from cdx_harvester.cdx_common import DomainConfig

CONFIG = DomainConfig(
    name="finances_news",
    cdx_url_pattern="fnh.ma/*",  # finances-news.ma redirects to fnh.ma
    date_from="20210101",
    exclude_regex=r"/(tag|category|auteur|author|page|search|feed|wp-admin|wp-content|amp)(/|$|\?)|\?page=",
)
