package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
)

func main() {
	var wg sync.WaitGroup

	recieveOrderCh := recieveOrder()
	validOrderCh, InvalidOrderCh := validateOrder(recieveOrderCh)
	reservedInvCh := reserveOrder(validOrderCh) // single producer multiple consumer
	fillOrderCh := fillOrder(reservedInvCh)     // single consumer multiple producer
	wg.Add(2)

	go func(InvalidOrderCh <-chan invalidOrder) {
		for order := range InvalidOrderCh {
			fmt.Printf("Received Invalid Order: %v, Issue is : %v\n", order.Order, order.err)
		}
		wg.Done()
	}(InvalidOrderCh)

	go func(fillOrderCh <-chan order) {
		for order := range fillOrderCh {
			fmt.Printf("Order has completed : %v\n", order)
		}
		wg.Done()
	}(fillOrderCh)

	wg.Wait()

}

func fillOrder(in <-chan order) <-chan order {
	out := make(chan order)
	go func() {
		for o := range in {
			o.Status = filled
			out <- o
		}
		close(out)
	}()
	return out
}

func reserveOrder(in <-chan order) <-chan order {
	out := make(chan order)
	const workers = 3
	// This demonstrates multiple worker single consumer in case of fillOrders recieving from reserveOrders
	// In case of recieve order calling validateOrder this demonstrates multiple consumer single worker
	// You can adjust value of workers as per your need
	var wg sync.WaitGroup
	wg.Add(3)
	for i := 0; i < workers; i++ {
		go func() {
			for o := range in {
				o.Status = reserved
				out <- o
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func validateOrder(in <-chan order) (<-chan order, <-chan invalidOrder) {
	// out & invalid are initiated inside the function &
	// returned as recieve only channels because channel will already have message in the buffer
	// in is receive-only channel as message from the channel is being received
	out := make(chan order)
	invalid := make(chan invalidOrder, 1)
	go func() {
		for order := range in {
			if order.Quantity > 0 {
				out <- order
			} else {
				invalid <- invalidOrder{Order: order, err: errors.New("invalid quantity, quantity should not be less than 0")}
			}
		}
		close(out)
		close(invalid)
	}()
	return out, invalid
}
func recieveOrder() <-chan order {
	// out is initiated inside the function & returned as reciever-only channel
	// sending to out will throw error after returning from this function
	out := make(chan order)
	go func() {
		for _, rawOrder := range rawOrders {
			var newOrder order
			err := json.Unmarshal([]byte(rawOrder), &newOrder)
			if err != nil {
				log.Print(err)
				continue
			}
			out <- newOrder
		}
		close(out)
	}()
	return out
}

var rawOrders = []string{
	`{"productCode": 1111, "quantity" : -5, "status": 1}`,
	`{"productCode": 1112, "quantity" : 58.67, "status": 1}`,
	`{"productCode": 1113, "quantity" : 765.78654, "status": 1}`,
}
