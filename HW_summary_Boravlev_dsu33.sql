select city, count from (
	select airport_code, city, count(airport_code) over (partition by city)
	from airports) a 
where count > 1
group by 1, 2

-- в оконной функции в подзапросе посчитал колич-во а/п с группировкой по городам и вывел с условием, что кол-во > 1;

select airport_name, q.aircraft_code 
from airports a2 
join flights f on f.arrival_airport= a2.airport_code 
join (
	select * from (
		select *, row_number () over (order by range desc)
		from aircrafts) a 
	where row_number = 1) q on q.aircraft_code = f.aircraft_code 
group by 1, 2

-- на самом нижнем уровне вложения проранжировал в окне таблицу aircraft по range.
-- т.к. к окну нельзя применить условие, на след уровне отфильтровал борт с максимальным range 
-- вывел имя аэропорта и код борта из подзапроса Q, т.к. аэропорт соединяется с самолетом через таблицу flights
-- сделал inner join, чтобы в выборку попали только те значения рейсов, где код борта совпадает к кодом из фильтрованной 
-- таблицы Q
-- так как в каждом аэропорту этот класс самотетов выполняет разные рейсы, понадобилась группировка, чтобы убрать дубли
-- как было сказано аэропорт прибытия и аэропорт вылета равнозначны, т.к. если есть прибытие, то будет и вылет, поэтому 
-- можно взять какой-нибудь один;

select *, (actual_departure - scheduled_departure) as delay 
from flights f 
where status = 'Departed' or status = 'Arrived'
order by delay desc
limit 10

-- опаздывать вылетом могут только рейсы, у которых вылет уже произошел, поэтому из всей совокупности оставляем только
-- статус "департед" и "эррайвд", вводим вычислимое значение "вылет фактич" - "вылет по расписанию", получаем интервал
-- который означает время задержки вылета, сортировка по этому интервалу по убыванию, верхние 10 строк это рейсы с 
-- максимальным этим интервалом

select book_ref, t.ticket_no, boarding_no
from tickets t 
left join boarding_passes bp on bp.ticket_no = t.ticket_no 
where boarding_no is null

-- брони живут в таблице tickets, посадочные талоны в boarding_passes, соединяю через left join, чтобы в выводе 
-- оказались все существующие билеты, а не только те, что есть в boarding_passes.
-- с помощью фильтрации выясняется, что по 127899 билетам из 366733 не выдано посадочных, что, вроде бы, дофига,
-- но я не вижу, что еще здесь можно учесть;

with cte1 as(
	select distinct (flight_id), count(boarding_no) over (partition by flight_id) as pass_no
	from boarding_passes bp 
	order by 1),
cte2 as(
	select f.flight_id, aircraft_code, departure_airport, actual_departure, pass_no,
	sum(pass_no) over (partition by departure_airport, actual_departure::date order by actual_departure) nakopit
	from cte1 c1
	right join flights f on f.flight_id = c1.flight_id
	where status = 'Departed' or status = 'Arrived')
select flight_id, c2.aircraft_code, departure_airport, actual_departure,  pass_no,
	nakopit, seats_c, seats_c - pass_no empty_seats, 
	round((((seats_c - pass_no)::numeric / seats_c::numeric) * 100)::numeric, 1) empty_procent
from cte2 c2
join 
	(select distinct (aircraft_code), count(seat_no) over (partition by aircraft_code) seats_c
	from seats s ) acs
	on acs.aircraft_code = c2.aircraft_code 
order by 3, 4

--в цте 1 из таблицы boarding_passes посчитал по каждому полету количество посадочных талонов,
--считаем, что все пассажиры, которым выдали посадочный - улетели. Использовал ДИСТИНКТ, потому 
-- что хотелось сократить количество строк, а групп бай по логике запроса работает раньше, 
-- чем окно и хотел, чтобы я добавил в группировку boarding_no, а это уникальные значения и группировать по ним плохо
-- в цте2 взял таблицу flights, соединил лефт джойном, потому что количество даже отфильтрованных улетевших и прибывших рейсов
-- больше, чем количество уникальных рейсов в таблице boarding_passes. Т.е какие-то рейсы пустыми полетели, что ли? 
-- в общем, решил. пусть они тоже будут. Также в цте2 с помощью значения pass_no, которое получил в цте1
-- посчитал в окне накопительную сумму улетевших пассажиров с группировкой по аэропорту вылета и дате вылета и сортировкой по времени вылета.
--в последнем селекте подтянул таблицу seats, где в подзапросе посчитал количество посадочных мест в каждом типе самолета
-- и уже арифметически просто вычислил заданные метрики;

explain analyze --5354
select distinct (aircraft_code), ac_no, count, round(((ac_no::numeric / count::numeric) * 100)::numeric, 1)
from(
	select flight_id, aircraft_code, count(flight_id) over (partition by aircraft_code) as ac_no, count(flight_id) over ()
	from flights) f
	
--улучшил этот запрос, тут же можно группировать, окно в подзапросе выполняется;
	
--explain analyze --4857
select aircraft_code, ac_no, count, round(((ac_no::numeric / count::numeric) * 100)::numeric, 1)
from(
	select flight_id, aircraft_code, count(flight_id) over (partition by aircraft_code) as ac_no, count(flight_id) over ()
	from flights) f
group by 1, 2, 3
	
-- сперва показалось, что все просто, но оказалось, что все сложно. Я так и не понял, почему не получилось вычислить общее
-- количество рейсов с помощью count(*), а пришлось хитрить с окном.
-- делал приведение к типу данных numeric, чтобы получить корректные результаты округления. 
-- также куда-то пропал aircraft_code '320'. Я проверил, в таблице flights нет рейсов с таким типов самолетов,
-- тогда было бы желательно вывести его со значением NULL, но я не понял, как это сделать;	


insert into ticket_flights (ticket_no, flight_id, fare_conditions, amount)
values ('0005433367255', 30625, 'Business', 9000)

--отредактировал таблицу ticket_flights, чтобы добавить бизнес дешевле эконома - запрос работает

--explain analyze
with cte1 as (
	select *
	from (
		select flight_id, fare_conditions, tf.amount as amount_b, row_number () over (partition by flight_id order by amount)
		from ticket_flights tf 
		where fare_conditions = 'Business') b
	where row_number = 1),
cte2 as(
	select *
from (
	select flight_id, fare_conditions, tf.amount as amount_e, row_number () over (partition by flight_id order by amount desc)
	from ticket_flights tf 
	where fare_conditions = 'Economy') e
where row_number = 1)
select c2.flight_id,amount_e, amount_b, amount_e - amount_b, f2.arrival_airport, a.city
from cte2 c2
join cte1 c1 on c1.flight_id = c2.flight_id
join flights f2 on f2.flight_id = c2.flight_id
join airports a on a.airport_code =f2.arrival_airport 
where amount_e - amount_b > 0

-- в цте1 фильтровал рейсы класса Бизнес, в оконной функции ранжировал внутри каждого рейса по стоимости по возрастанию,
-- т.о. роу_намбер = 1 это самый дешевый билет класса Бизнес на этом рейсе;
-- в цте2 аналогично нахожу самый дорогой билет класса Эконом на каждом рейсе;
-- соединяю с помощью иннер джойн, потому что нужно найти рейсы, на которых есть и класс Бизнес, и класс Эконом;
-- добавляю вычисляемое значение "стоимость эконом" - "стоимость бизнес" и вывожу с условием, что эта разность больше 0.
-- (моя логика говорит, что таких казусов нет в данной таблице, поэтому я не стал подтягивать аэропорты и города, чтобы
-- вывести их название, ибо их нет.) - не актуально
-- очень тяжелый запрос, покомментируйте,пожалуйста

create view dest_view as(
select destinations
from (
	select departure_airport, arrival_airport, a1.city, a2.city, array [a1.city, a2.city] destinations
	from flights f 
	join airports a1 on a1.airport_code = f.departure_airport 
	join airports a2 on a2.airport_code = f.arrival_airport 
	where a1.city > a2.city) d)

select citiz 
from(
	select a1.city, a2.city, array[a1.city, a2.city] citiz
	from airports a1, airports a2
	where a1.city > a2.city) c
except 
select *
from dest_view dv 

-- в пердставлении формирую массив из пары а/п вылета - а/п прилета. Получается 16К+ неуникальных значений,
-- но это все то, что летает напрямую
-- затем с помощью декартова произведения нахожу все возможные сочетания пар городов, в которых есть 
-- аэропорты, также убираю зеркальные значения и формирую массив
-- с помощью оператора except из всех комбинаций пар городов отфильтровываются пары, которые связаны прямыми рефсами
-- т.о. из 5352 пар остается 4792 пары не связанных прямыми авиаперелетами, что правдоподобно в целом;

select departure_airport, arrival_airport, n.aircraft_code, n.range, 
	acosd(sind(lat_dep)*sind(lat_arr) + cosd(lat_dep)*cosd(lat_arr)*cosd(long_dep - long_arr))*6371,
	case 
		when acosd(sind(lat_dep)*sind(lat_arr) + cosd(lat_dep)*cosd(lat_arr)*cosd(long_dep - long_arr))*6371 <= n.range then 'OK'
		else 'NOT OK'
	end
	from 
		(select departure_airport, arrival_airport, a.aircraft_code, a.range, 
			a1.longitude long_dep, a1.latitude lat_dep, a2.longitude long_arr, a2.latitude lat_arr
		from flights f
		join airports a1 on a1.airport_code = f.departure_airport 
		join airports a2 on a2.airport_code = f.arrival_airport
		join aircrafts a on a.aircraft_code = f.aircraft_code 
		where departure_airport > arrival_airport) n 
		
--рейсы в таблице flights, координаты в таблице airports. 
-- соединяю airports с flights два раза, сначала по аэропортам вылета, затем по аэропортам прилета,
-- чтобы получить координаты и тех, и других. 
-- сравнение в операторе WHERE позволяет избавится от зеркальных пар значений. 
-- пары остаются не уникальные, потому что направления одно и то же, а рейсы разные, но это не самое главное пока что.
-- тригономентрически вычисляю расстояние по координатам. Вычисляется некорректно, хотя коордтнаты вроде бы правильные, я проверял.
-- в условии CASE делаю проверку.
-- я еще поработаю над этим заданием, сейчас нужно лететь в командировку. Я разобрался, что координаты указаны в градусах и нужно
-- работать через sind/cosd, но дальше не получается продвинуться.
		
select distinct (array [city_dep, city_arr]), n.aircraft_code, n.range, 
	acos(sind(lat_dep)*sind(lat_arr) + cosd(lat_dep)*cosd(lat_arr)*cosd(long_dep - long_arr))*6371,
	case 
		when acos(sind(lat_dep)*sind(lat_arr) + cosd(lat_dep)*cosd(lat_arr)*cosd(long_dep - long_arr))*6371 <= n.range then 'OK'
		else 'NOT OK'
	end
	from 
		(select departure_airport, a1.city city_dep, arrival_airport, a2.city city_arr, a.aircraft_code, a.range, 
			a1.longitude long_dep, a1.latitude lat_dep, a2.longitude long_arr, a2.latitude lat_arr
		from flights f
		join airports a1 on a1.airport_code = f.departure_airport 
		join airports a2 on a2.airport_code = f.arrival_airport
		join aircrafts a on a.aircraft_code = f.aircraft_code 
		where departure_airport > arrival_airport) n 
order by 1
		
-- растояние вычисляется, в скобках с помощью операторов sind/cosd (потому что координаты указаны в градусах)
--вычисляю значение cos D, но оно уже не в градусах получается, поэтому чтобы обратить его, нужно использовать 
-- acos а не acosd.