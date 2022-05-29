# Functions for data scraping process #
import pandas as pd
import time
from selenium import webdriver
from bs4 import BeautifulSoup

# Take nothing as input and outputs a raw dataframe
def scrapeQuintoAndar() -> pd.DataFrame:
    # Instantiate a google chorme drive
    driver = webdriver.Chrome(executable_path=r"C:\Users\PedroMiyasaki\AppData\Local\SeleniumBasic\chromedriver.exe")

    # get this link
    driver.get("https://www.quintoandar.com.br/alugar/imovel/curitiba-pr-brasil/amp")

    # Wait for page loading
    time.sleep(5)

    # Def pre scrol variables
    scroll_pause_time = 1 # You can set your own pause time. My laptop is a bit slow so I use 1 sec
    screen_height = driver.execute_script("return window.screen.height;")   # get the screen height of the web
    i = 1

    # Scrol until the end of page
    while True:
        # scroll one screen height each time
        driver.execute_script("window.scrollTo(0, {screen_height}*{i});".format(screen_height=screen_height, i=i))  
        i += 1
        time.sleep(scroll_pause_time)
        # update scroll height each time after scrolled, as the scroll height can change after we scrolled the page
        scroll_height = driver.execute_script("return document.body.scrollHeight;")  
        # Break the loop when the height we need to scroll to is larger than the total scroll height
        if (screen_height) * i > scroll_height:
            break

    # Take HTML from main div
    div_main = driver.find_element_by_xpath("/html/body/main/amp-list/div[2]")
    html_content = div_main.get_attribute('outerHTML')
    soup = BeautifulSoup(html_content, 'html.parser')

    # Take list of "cards"
    lista_cards = soup.find_all('div', class_='card item')
    ll = []
    c = 0
    for i in lista_cards:
        text = lista_cards[c].get_text()
        ll.append(text)
        c += 1

    # Male copy to avoid
    df = {
        'C0': [],
        'C1': [],
        'C2': [],
        'C3': [],
        'C4': [],
        'C5': [],
        'C6': [],
        'C7': [],
        'C8': []
    }

    # For every element on the list
    for element in ll:
        element = element.split('\n')
        filtered_elements = []
        for filtered_element in element:
            if len(filtered_element) > 1:
                filtered_elements.append(filtered_element)
        for item in filtered_elements:

            # faça uma string com eles
            if len(item.strip()) > 1:
                item_str = item.strip().lower()
                if 'anúncio' in item_str:
                    df['C0'].append(item)

                elif 'apartamento' in item_str or 'casa' in item_str or 'kitnet' in item_str:
                    df['C1'].append(item)

                elif 'exclusivo' in item_str or 'originals' in item_str:
                    df['C2'].append(item)

                elif 'curitiba' in item_str:
                    df['C4'].append(item)

                elif 'm²' in item_str:
                    df['C5'].append(item)

                elif 'dorms' in item_str:
                    df['C6'].append(item)

                elif 'aluguel' in item_str:
                    df['C7'].append(item)

                elif 'total' in item_str:
                    df['C8'].append(item)
                
                else:
                    df['C3'].append(item)

        len_of_keys = [len(df[key]) for key in df]
        max_key_lenght = max(len_of_keys)

        for key in df:
            if len(df[key]) < max_key_lenght:
                df[key].append('vazio') 


    # Make dataframe
    df = pd.DataFrame(df)

    # Return it
    return df