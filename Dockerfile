FROM python:3.8

RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -\
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'\
    &&  apt-get -y update\
    && apt-get install -y google-chrome-stable
#
# RUN apt-get update && apt-get install -y firefox-esr
#
COPY . .

RUN pip install -r requirements.txt

CMD ["python", "scraper/scraper.py"]