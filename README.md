# Программа выгрузки данных потребления
Компоненты для манипуляции и нормализации данных из сырых источников. Данные компоненты позволяют быстро сохранить данные из внешних источников в csv файл и в дельнейшем загрузить их в БД.

* **Выгружает почасовые данные**
* **Выгружает данные в разрезе субъектов РФ**

Из-за высокой нагрузки на API и большого числа запросов код вынесен в оффлайн приложение. При этом:

* Данное приложение в целом является законченным
* Перестройка с оффлайн режима на обобщенный потребует минимальных изменений
* Выгрузка работает стабильно и детерменировано
* Запуск может быть легко автоматизирован при помощи cron-подобных инструментов

## Описание технологий

| Поле | Значение | Комментарий |
| ---------------- | ----------- | -------------------------------------------|
| Язык программирования | Java 11 | |
| WEB-Framework | Spring Boot | [Документация](https://spring.io/projects/spring-boot) |
| Работе с JSON | Jackson | [Репозиторий GitHub](https://github.com/FasterXML/jackson) |

## Описание решения

- [Обобщенный блок, отвечающий за формирование CSV-файлов из выгружаемых данных](https://github.com/graphometrica/power_parser/blob/master/src/main/java/ai/graphometrica/dataparser/power/CSVAppender.java)
- [Блок, отвечающий за загрузку в БД общих данных](https://github.com/graphometrica/power_parser/blob/master/src/main/java/ai/graphometrica/dataparser/power/PostgresImport.java)
- [Блок, отвечающий за предобработку и загрузку в БД данных температуры](https://github.com/graphometrica/power_parser/blob/master/src/main/java/ai/graphometrica/dataparser/power/PostgresImportTemp.java)
- [Блок, отвечающий за предобработку и загрузку в БД данных потребления](https://github.com/graphometrica/power_parser/blob/master/src/main/java/ai/graphometrica/dataparser/power/PostgresImportPSID.java)
- [Блок, отвечающий за парсинг API](https://github.com/graphometrica/power_parser/blob/master/src/main/java/ai/graphometrica/dataparser/power/PowerApplication.java)

## Описание принципа работы

В основе приложения идет обращение к API сайта [https://br.so-ups.ru/](https://br.so-ups.ru/), ссылка на который была предоставлена вместе с описанием кейса. Выгружаются почасовые данные потребления (и не только) по каждому из регионов.
