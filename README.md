# SQL_sample
##Исследование демонстрационной [базы данных](edu.postgrespro.ru/booking.pdf) авиаперелетов для СУБД PostgreSQL

В работе использовался тип подключения **local**

![ER_diagramm](https://github.com/nboravlev/SQL_sample/blob/main/ER_diagram.PNG)

БД содержит информацию об авиаперелетах по России за 3 месяца. Соостоит из 7 таблиц, 1 представления и 1 материализованного представления.

Цель исследования, бизнес-задачи, которые можно решить с помощью БД:

- Исследовать степень загрузки бортов по направлениям, может оказаться, что по каким-то направлениям летают избыточно большие самолеты, и можно заменить их на менее большие с целью экономии топлива;
- Выявить аэропорты с аномально большим количеством нарушений расписания, возможно удасться обнаружить корреляцию и понять причины;
- Строить программу лояльности на основании информации о том, кто, сколько и куда летает;
- Исследовать нагрузку на воздушное судно, чтобы планировать техническое обслуживание;
- Исследовать соотношение времени эксплуатации воздушного судна и времени на земле, возможно удасться обнаружить аномалии.
