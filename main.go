package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

//Шаг 1: Подготовка структур данных
//● Определите структуру Job для задания (например, с полями ID и URL).
//● Определите структуру Result для результата (например, с полями Job, Status, Duration).

type Job struct {
	id  int
	url string
}

type Result struct {
	job      Job
	status   string
	duration time.Duration
}

// Шаг 2: Создайте функцию-воркер
// ● Функция должна принимать на вход канал для чтения заданий (<-chan Job), канал для записи результатов (chan<- Result) и *sync.WaitGroup.
// ● В функции используйте цикл for range по каналу с заданиями.
// ● Внутри цикла вызовите функцию-заглушку для имитации запроса (например, time.Sleep(randomDuration)).
// ● Отправьте результат обработки в канал результатов.
func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for job := range jobs {
		results <- process(job)
	}
}

// Шаг 3: Реализуйте паттерн Fan-out / Worker Pool
// ● В функции main создайте канал для заданий (jobs) и канал для результатов (results). Подумайте о необходимости их буферизации.
// ● Используя sync.WaitGroup, запустите фиксированное количество горутин-воркеров (например, 5). Каждая горутина должна выполнять функцию из Шага 2.

// Шаг 4: Реализуйте паттерн Fan-in (сбор результатов)
// ● После запуска воркеров вам нужно отправить все задания в канал jobs и закрыть его, чтобы воркеры знали, когда остановиться.
// ● Запустите в отдельной горутине код, который с помощью wg.Wait() дождётся завершения всех воркеров и затем закроет канал results.
func main() {
	jobs := make(chan Job)
	results := make(chan Result)
	wg := new(sync.WaitGroup)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go worker(jobs, results, wg)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	fill(jobs)
	for result := range results {
		fmt.Println(result.job.id, result.job.url, result.status, result.duration)
	}
}

// Шаг 5: Генерация заданий и агрегация результатов
// ● Наполните канал jobs заданиями на основе вашего списка URL.
// ● В главной горутине (в функции main) используйте цикл for range по каналу results, чтобы прочитать все результаты и, например, собрать их в слайс.
// ● Выведите финальный отчёт: список всех обработанных URL с их временем выполнения и общую статистику (например, среднее время, количество успешных операций).
func fill(jobs chan<- Job) {
	defer close(jobs)
	for i := 0; i < 5; i++ {
		jobs <- Job{i, "http://google.com"}
	}
}

func process(job Job) Result {
	randomDuration := time.Duration(rand.Intn(1000)) * time.Millisecond
	time.Sleep(randomDuration)
	return Result{
		job:      job,
		status:   "processed",
		duration: randomDuration,
	}
}
