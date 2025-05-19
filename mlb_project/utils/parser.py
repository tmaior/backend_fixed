import pandas as pd

def parse_table(table):
    thead = table.find('thead')
    if thead:
        headers = [th.get_text(strip=True) for th in thead.find_all('th')]
    else:
        first = table.find('tbody').find('tr')
        headers = [f"Col{i+1}" for i,_ in enumerate(first.find_all('td'))]
    rows = []
    for tr in table.find('tbody').find_all('tr'):
        cells = [td.get_text(strip=True) for td in tr.find_all('td')]
        cells += [''] * (len(headers) - len(cells))
        rows.append(cells)
    return pd.DataFrame(rows, columns=headers)
