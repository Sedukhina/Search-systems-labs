from urllib.parse import urlparse

def GetDomainAndRelativePath(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    if domain.startswith("www."):
        domain = domain[4:]
    relative_link = parsed_url.path

    return domain, relative_link 