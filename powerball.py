'''
sudo apt-get update -y && sudo apt-get install vim python3 python3-dev python3-pip -y && pip3 install requests bs4 && mkdir csv
vim powerball.py
nohup python3 powerball.py &
'''
import re
import csv
import time
import queue 
import threading
import calendar
import requests
from bs4 import BeautifulSoup

SLEEP_TIME = 10
MAX_THREAD = 1
YEAR_START = 2014
YEAR_END = 2018
DIR = './csv/'


# threading 패키지의 Thread 클래스를 상속받는다.
class Crawler(threading.Thread) :
  
  # Crawler 클래스는 date(날짜 str)를 생성 매개변수로 받는다.
  def __init__(self, theQueue=None) :
    
    # 부모 클래스 초기화
    threading.Thread.__init__(self)
    
    # 클래스 변수 초기화
    self.theQueue = theQueue
    self.date = ''
    self.dataset = []
  
  
  
  # run 함수는 Thread 클래스의 시작 함수이다.
  def run(self) :
    
    while True:
    
      # 큐에서 data 가져오기
      self.date = self.theQueue.get()
      self.dataset = []
      
      # 크롤링 하기
      self.crawling()
      
      # 큐에서 가저온 data를 끝냈다.
      self.theQueue.task_done()
  
  
  
  # 크롤링 함수
  def crawling(self):
    
    # 해당 날짜의 번호를 모두 수집 할 때까지 무한 반복 (최소1 부터 최대 40페이지로 설정) 한다.
    for page in range(1, 41) :

      # time.sleep(3) for 1 page
      time.sleep(SLEEP_TIME)

      # URL 가져온다.
      req = requests.get('http://m.nlotto.co.kr/gameInfo.do?method=powerWinNoList&nowPage='+str(page)+'&searchDate='+str(self.date)+'&sortType=num')
      # HTML 가져온다.
      html = req.text

      # 가져온 HTML을 BeautifulSoup를 이용하여 파싱한다.
      soup = BeautifulSoup(html, 'html.parser')

      # CSS Selector를 통해 html요소들을 찾아낸다. 당첨 테이블의 tr들을 가져온다.
      list_tr = soup.select('#frm > table.tblType1_1 > tbody > tr')

      for tr in list_tr :
        tds = tr.find_all('td')

        # ts 중에 td가 1개짜리가 있기때문에 td가 9개만 반드시 처리한다.
        if len(tds) == 9 :
          # 추첨일, 회차, 당첨번호, 파워볼, 숫자합, 홀/짝, 대/중/소, 숫자합구간, 파워볼구간
          day = tds[0].string
          turn = tds[1].string
          number = re.sub('[^0-9]', '', tds[2].find('script').string)
          powerball = re.sub('[^0-9]', '', tds[3].find('img').get('src'))
          numbersum = tds[4].string
          oe = tds[5].string.replace('\n', ' ').replace('\r', '').replace('\t', '').replace(' ', '')
          bms = tds[6].string.replace('\n', ' ').replace('\r', '').replace('\t', '').replace(' ', '')
          numbersumsection = tds[7].string.replace('\n', ' ').replace('\r', '').replace('\t', '').replace(' ', '')
          powerballsection = tds[8].string.replace('\n', ' ').replace('\r', '').replace('\t', '').replace(' ', '')

          self.dataset.append([day, turn, number, powerball, numbersum, oe, bms, numbersumsection, powerballsection])

      # req를 닫아준다.
      req.close()

    # def run이 종료을 종료하기 전에 csv파일을 생성하고, 중복된 영역을 포함한 dataset을 입력한다.
    f = open(DIR+self.date+'.csv', 'w', encoding='utf-8', newline='')
    wr = csv.writer(f)
    for data in self.dataset:
      wr.writerow(data)
    f.close()
        

# MAIN
q = queue.Queue()
dates = []

# 날짜 생성
for year in range(YEAR_START, YEAR_END+1) :
  for month in range(1, 12+1) :
    days = calendar.monthrange(year, month)
    for day in range(1, days[1]+1):
      dates.append(str(year)+str(month).zfill(2)+str(day).zfill(2))

# 스레드 생성 및 시작
for _ in range(MAX_THREAD):
    thread = Crawler(theQueue=q)
    thread.start() # thread started. But since there are no tasks in Queue yet it is just waiting.

# 날짜를 큐에 넣기
for d in dates:       
    q.put(d) # as soon as task in added here one of available Threads picks it up
