package batch

import (
	"sync"
	"time"
)

type user struct {
	ID int64
}

func getOne(id int64) user {
	time.Sleep(time.Millisecond * 100)
	return user{ID: id}
}

func getBatch(n int64, pool int64) (res []user) {
	checkArgs(n, pool)
	pool = max(min(pool, n), 1)

	var resLock = new(sync.Mutex)
	res = make([]user, 0, n)

	var idsCh = make(chan int64, pool)
	var resCh = make(chan user, pool)

	genWg := new(sync.WaitGroup)
	resWg := new(sync.WaitGroup)

	for i := int64(0); i < pool; i++ {
		genWg.Add(1)
		go getOneAsync(idsCh, resCh, genWg)

		resWg.Add(1)
		go handleResultsAsync(resCh, resLock, &res, resWg)
	}

	genWg.Add(1)
	go fillTasksAsync(n, idsCh, genWg)

	genWg.Wait()
	close(resCh)
	resWg.Wait()

	return res
}

func handleResultsAsync(results chan user, resLock *sync.Mutex, res *[]user, wg *sync.WaitGroup) {
	defer wg.Done()

	for u := range results {
		resLock.Lock()
		*res = append(*res, u)
		resLock.Unlock()
	}
}

func fillTasksAsync(n int64, tasks chan<- int64, wg *sync.WaitGroup) {
	defer close(tasks)
	defer wg.Done()

	for i := int64(0); i < n; i++ {
		tasks <- i
	}
}

func getOneAsync(tasks <-chan int64, results chan<- user, wg *sync.WaitGroup) {
	defer wg.Done()

	for id := range tasks {
		results <- getOne(id)
	}
}

func checkArgs(n int64, pool int64) {
	if n < 0 {
		panic(n)
	}

	if pool < 0 {
		panic(pool)
	}
}

func max(a int64, b int64) int64 {
	if a > b {
		return a
	}

	return b
}

func min(a int64, b int64) int64 {
	if a < b {
		return a
	}

	return b
}
