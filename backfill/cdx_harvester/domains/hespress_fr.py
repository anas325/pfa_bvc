from cdx_harvester.cdx_common import DomainConfig

CONFIG = DomainConfig(
    name="hespress_fr",
    cdx_url_pattern="fr.hespress.com/*",
    date_from="20210101",
    exclude_regex=r"/(tag|category|auteur|author|page|search|feed|amp)(/|$|\?)|\?page=",
)
