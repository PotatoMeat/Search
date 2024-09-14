import sqlite3
import numpy as np
import re
from bs4 import BeautifulSoup
import requests as req



class Seacher:
    connection = None
    cursor = None
    D = 0.85

    def dbcommit(self):
        """ Зафиксировать изменения в БД """
        self.connection.commit()

    def __init__(self, dbFileName):
        """  0. Конструктор """
        self.connection = sqlite3.connect(dbFileName)
        self.cursor = self.connection.cursor()

    def __del__(self):
        """ 0. Деструктор  """
        self.cursor.close()
        self.connection.close()

    def getWordsIds(self, queryString):

        queryString = queryString.lower()
        queryWordsList = queryString.split(" ")
        rowidList = list()

        for word in queryWordsList:
            sql = "SELECT rowid FROM wordList WHERE word =\"{}\" LIMIT 1; ".format(word)
            self.cursor.execute(sql)
            result_row = self.cursor.fetchone()

            if result_row != None:
                word_rowid = result_row[0]
                rowidList.append(word_rowid)
                print("  ", word, word_rowid)
            else:
                raise Exception("Одно из слов поискового запроса не найдено:" + word)
        return rowidList

    def getMatchRows(self, queryString):
        queryString = queryString.lower()
        wordsList = queryString.split(" ")

        wordsidList = self.getWordsIds(queryString)

        sqlFullQuery = """"""

        sqlpart_Name = list()  # имена столбцов
        sqlpart_Join = list()  # INNER JOIN
        sqlpart_Condition = list()  # условия WHERE

        for wordIndex in range(0, len(wordsList)):
            wordID = wordsidList[wordIndex]
            if wordIndex == 0:
                sqlpart_Name.append("""w0.fk_URLId    urlId  --идентификатор url-адреса""")
                sqlpart_Name.append("""   , w0.location w0_loc --положение первого искомого слова""")
                sqlpart_Condition.append("""WHERE w0.fk_wordId={}     -- совпадение w0 с первым словом """.format(wordID))

            else:
                if len(wordsList) >= 2:
                    sqlpart_Name.append(
                        """ , w{}.location w{}_loc --положение следующего искомого слова""".format(wordIndex,
                                                                                                   wordIndex))
                    sqlpart_Join.append("""INNER JOIN wordLocation w{}  -- назначим псевдоним w{} для второй из соединяемых таблиц
                       on w0.fk_URLId=w{}.fk_URLId -- условие объединения""".format(wordIndex, wordIndex, wordIndex))
                    sqlpart_Condition.append(
                        """  AND w{}.fk_wordId={} -- совпадение w{}... с cоответсвующим словом """.format(wordIndex,
                                                                                                       wordID,
                                                                                                       wordIndex))
                    pass
            pass
        sqlFullQuery += "SELECT "

        for sqlpart in sqlpart_Name:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        sqlFullQuery += "\n"
        sqlFullQuery += "FROM wordLocation w0 "

        for sqlpart in sqlpart_Join:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        for sqlpart in sqlpart_Condition:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        print(sqlFullQuery)
        cur = self.cursor.execute(sqlFullQuery)
        rows = [row for row in cur]

        return rows, wordsidList

    def normalizeScores(self, scores, smallIsBetter=0):

        resultDict = dict()  # словарь с результатом

        vsmall = 0.00001  # создать переменную vsmall - малая величина, вместо деления на 0
        minscore = min(scores.values())  # получить минимум
        maxscore = max(scores.values())  # получить максимум

        # перебор каждой пары ключ значение
        for (key, val) in scores.items():

            if smallIsBetter:
                # Режим МЕНЬШЕ вх. значение => ЛУЧШЕ
                # ранг нормализованный = мин. / (тек.значение  или малую величину)
                resultDict[key] = float(minscore) / max(vsmall, val)
            else:
                # Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ вычислить макс и разделить каждое на макс
                # вычисление ранга как доли от макс.
                # ранг нормализованный = тек. значения / макс.
                resultDict[key] = float(val) / maxscore

        return resultDict

    # Ранжирование. Содержимомое. 1. Частота слов.
    def frequencyScore(self, rowsLoc):
        """
        Расчет количества комбинаций искомых слов
        Пример встречается на странице  q1 - 10 раз,  q2 - 3 раза, Общий ранг страницы = 10*3 = 30 "комбинаций"
        :param rowsLoc: Список вхождений: urlId, loc_q1, loc_q2, .. слов из поискового запроса "q1 q2 ..." (на основе результата getmatchrows ())
        :return: словарь {UrlId1: общее кол-во комбинаций, UrlId2: общее кол-во комбинаций, }
        """

        countsDict = dict()
        curent_urlId = 0
        count = 0

        unique_comb = set(rowsLoc)
        unique_comb = sorted(unique_comb)

        for urls in unique_comb:
            if urls[0] != curent_urlId:
                if urls[0] != 0 and count != 0:
                    countsDict[curent_urlId] = count
                curent_urlId = urls[0]
                count = 0
            else:
                count+=1

        return self.normalizeScores(countsDict, smallIsBetter=0)

    def geturlname(self, id):
        self.cursor.execute("SELECT URL FROM URLList WHERE rowId = " + str(id))
        self.dbcommit()
        return  self.cursor.fetchone()[0]

    def getSortedList(self, queryString):
        rowsLoc, wordids = self.getMatchRows(queryString)
        m1Scores = self.frequencyScore(rowsLoc)
        rankedScoresList = list()
        for url, score in m1Scores.items():
            pair = (score, url)
            rankedScoresList.append(pair)

        rankedScoresList.sort(reverse=True)

        print("score, urlid, geturlname")
        for (score, urlid) in rankedScoresList[0:10]:
            print("{:.2f} {:>5}  {}".format(score, urlid, self.geturlname(urlid)))

        for i in range(0, 3):
            self.searchHTML(self.geturlname(rankedScoresList[i][1]), queryString.split(" "),
                            "Ansver" + str(i+1) + ".html")

    def getSortedListWithPR(self, queryString):
        rowsLoc, wordids = self.getMatchRows(queryString)
        m1Scores = self.frequencyScore(rowsLoc)
        rankedScoresList = list()
        for url, score in m1Scores.items():
            pair = (score, url)
            rankedScoresList.append(pair)

        prScoresList = list()
        m2Scores = self.pagerankScore(rowsLoc)
        for urlPR, scorePR in m2Scores.items():
            prScoresList.append(scorePR)

        for i in range(0, len(rankedScoresList)):
            rankedScoresList[i] = rankedScoresList[i] + (prScoresList[i],)

        rankedScoresList.sort(reverse=True)

        print("scoreFS, scorePR, urlid, geturlname")
        for (score, urlid, scorePR) in rankedScoresList[0:10]:
            print("{:.2f}     {:.2f}    {:>5}   {}".format(score, scorePR,urlid, self.geturlname(urlid)))

        for i in range(0 , len(rankedScoresList)):
            self.searchHTML(self.geturlname(rankedScoresList[i][1]), queryString.split(" "),
                            "Ansver"+str(i+1)+".html")

    def calculatePageRank(self, iterations=5):
        self.connection.execute('DROP TABLE IF EXISTS pagerank')
        self.connection.execute("""CREATE TABLE  IF NOT EXISTS  pagerank(
                            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                            urlid INTEGER,
                            score REAL
                        );""")
        self.connection.execute("DROP INDEX   IF EXISTS wordidx;")
        self.connection.execute("DROP INDEX   IF EXISTS urlidx;")
        self.connection.execute("DROP INDEX   IF EXISTS wordurlidx;")
        self.connection.execute("DROP INDEX   IF EXISTS urltoidx;")
        self.connection.execute("DROP INDEX   IF EXISTS urlfromidx;")
        self.connection.execute('CREATE INDEX IF NOT EXISTS wordidx       ON wordList(word)')
        self.connection.execute('CREATE INDEX IF NOT EXISTS urlidx        ON URLList(URL)')
        self.connection.execute('CREATE INDEX IF NOT EXISTS wordurlidx    ON wordLocation(fk_wordId)')
        self.connection.execute('CREATE INDEX IF NOT EXISTS urltoidx      ON linkBetweenURL(fk_ToURLId)')
        self.connection.execute('CREATE INDEX IF NOT EXISTS urlfromidx    ON linkBetweenURL(fk_FromURLId)')
        self.connection.execute("DROP INDEX   IF EXISTS rankurlididx;")
        self.connection.execute('CREATE INDEX IF NOT EXISTS rankurlididx  ON pagerank(urlid)')
        self.connection.execute("REINDEX wordidx;")
        self.connection.execute("REINDEX urlidx;")
        self.connection.execute("REINDEX wordurlidx;")
        self.connection.execute("REINDEX urltoidx;")
        self.connection.execute("REINDEX urlfromidx;")
        self.connection.execute("REINDEX rankurlididx;")

        self.connection.execute('INSERT INTO pagerank (urlid, score) SELECT rowId, 1.0 FROM URLList')
        self.dbcommit()

        self.cursor.execute("SELECT rowId FROM URLList")
        self.dbcommit()
        ids = self.cursor.fetchall()

        for k in iterations:
            for i in ids:#Обходим все страницы в БД и расчитываем ранги для каждой
                self.cursor.execute("SELECT fk_FromURLId FROM linkBetweenURL WHERE (fk_ToURLId = " + str(i[0]) + ")")#Получаем все id всех страниц, которые ссылаются на текущую
                self.dbcommit()
                refs = self.cursor.fetchall()
                rangs = []
                counts = []

                for j in refs: # Получаем все ранги и количества url на всех страницах
                    rangs.append((self.connection.execute("SELECT score FROM pagerank WHERE (urlid = " + str(j[0]) + ")")).fetchone()[0])
                    counts.append((self.connection.execute("SELECT count(*) FROM linkBetweenURL WHERE (fk_FromURLId = " + str(j[0]) + ")")).fetchone()[0])

                urlid = i[0]
                sum_pr = 0 #Сумма всех PR
                mult = np.prod(counts)#Перемножаем "все количества" url на всех страницах для приведения к общем знаменателю
                for j in range(0, len(refs)):
                    sum_pr+=rangs[j]*mult/(counts[j]**2)
                pr = (1 - self.D) + self.D *sum_pr #PR текущей страницы
                self.connection.execute('UPDATE pagerank SET score=%f WHERE urlid=%d' % (pr, urlid))
            self.dbcommit()

    def pagerankScore(self, rows):
        unique_comb = set(rows)
        unique_comb = sorted(unique_comb)
        countsDict = dict()
        for cur_id in unique_comb:
            countsDict[cur_id[0]] = (self.cursor.execute("SELECT score FROM pagerank WHERE urlid = " + str(cur_id[0]))).fetchone()[0]
        normalizedscores = self.normalizeScores(countsDict, smallIsBetter=0)# нормализовать отностительно максимума
        return normalizedscores

    def createMarkedHtmlFile(self, markedHTMLFilename, testText, testQueryList):

        # Прeобразование текста к нижнему регистру
        testText = testText.lower()
        for i in range(0, len(testQueryList)):
            testQueryList[i] = testQueryList[i].lower()

        # Получения текста страницы с знаками переноса строк и препинания. Прием с использованием регулярных выражений
        wordList = re.compile("[\\w]+|[\\n.,!?:—]").findall(testText)#Заменить Beautyful soup

        # Получить html-код с маркировкой искомых слов
        htmlCode = self.getMarkedHTML(wordList, testQueryList)
        print(htmlCode)

        # сохранить html-код в файл с указанным именем
        file = open(markedHTMLFilename, 'w', encoding="utf-8")
        file.write(htmlCode)
        file.close()

    def getMarkedHTML(self, wordList, queryList):
        for string in queryList:
            wordList = wordList.replace(string, "<mark>"+str(string)+"</mark>")
        return wordList

    def searchHTML(self, url, queryList, fileName):
        resp = req.get(url)
        soup = BeautifulSoup(resp.text, 'lxml')
        bs_text = []
        for words in soup.find_all('p'):
            bs_text.append(words.getText())
        for str in bs_text:
            str.lower()
        for i in range(0, len(queryList)):
            queryList[i] = queryList[i].lower()

        fullHTML = """<!DOCTYPE html>
                <html>
                  <head> 
                    <meta charset="utf-8"> 
                    <title>Страница Ansver </title>
                  </head>
                  <body> 
                     """
        for str in bs_text:
            fullHTML+= "<p>"+ self.getMarkedHTML(str, queryList) + "</p>" + "\n"

        fullHTML += """</body></html>"""

        file = open(fileName, 'w', encoding="utf-8")
        file.write(fullHTML)
        file.close()


def main():
    PATH_DB = 0
    mySeacher = Seacher(PATH_DB)
    mySearchQuery = "-"
    #mySeacher.getSortedList(mySearchQuery)
    #mySeacher.calculatePageRank()
    mySeacher.getSortedListWithPR(mySearchQuery)
    #mySeacher.searchHTML("https://ru.wikipedia.org/wiki/Сера", mySearchQuery.split(" "))
    # rowsLoc, wordsidList = mySeacher.getMatchRows(mySearchQuery)
    #
    # print("-----------------------")
    # print(mySearchQuery)
    # print(wordsidList)
    # for location in rowsLoc:
    #     print(location)

main()
