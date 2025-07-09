seo域名爬取与筛选脚本



expireddomains.py启动打开expireddomains域名网站，进行手动登录，在控制台按回车进行下一步爬取操作



爬取依据，根据网站提前设置的筛选条件，进行，示例：https://member.expireddomains.net/domains/combinedexpired/?savedsearch_id=546720&fabirth_year=2024&fwhoisage=2025&fbl=500&facr=35&fadult=1&fwhois=22&o=domainpop&r=d

筛选条件：仅可用域名，BL反链>500，WBY年份MAX2024，ABY年份MAX2025，ACR参数>35

可设置爬取页数
 MAX_PAGES = 100

爬取速度过快，可能触发30秒限制

filter_domains.py筛选脚本

筛选爬取脚本保存的domains.csv内的域名
进行初步筛选  或 “极品”筛选

可设置本地代理池，需搭建本地代理池

PROXY_PORTS = range(30001, 30852)
PROXIES_LIST = [
    {
        "http": f"http://127.0.0.1:{port}",
        "https": f"http://127.0.0.1:{port}"
    }
    for port in PROXY_PORTS
]



线程数

MAX_WORKERS = 10 

域名后缀限制，只筛选特点后缀

ACCEPTED_TLDS = ["com", "net", "org"]





注意：expireddomains中的域名，实际情况可能与SEO工具或网站中查询的不匹配（expireddomains中存在大量反链，实际查出质量过低情况）