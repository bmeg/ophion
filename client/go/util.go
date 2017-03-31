
package ophion

import (
	"context"
	"google.golang.org/grpc"
)


func Connect(address string) (QueryClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
  if err != nil {
    return nil, err
  }
  out := NewQueryClient(conn)
  return out, err
}

type QueryBuilder struct {
	client QueryClient
	query  []*GraphStatement
}

func Query(client QueryClient) QueryBuilder {
	return QueryBuilder{client, []*GraphStatement{}}
}

func (q QueryBuilder) V(id ...string) QueryBuilder {
	if len(id) > 0 {
		return QueryBuilder{ q.client, append(q.query, &GraphStatement{&GraphStatement_V{id[0]}}) }
	} else {
		return QueryBuilder{ q.client, append(q.query, &GraphStatement{}) }
	}
}

func (q QueryBuilder) Execute() (chan *ResultRow, error) {
	tclient, err := q.client.Traversal(context.TODO(), &GraphQuery{q.query})
	if err != nil {
		return nil, err
	}
	out := make(chan *ResultRow, 100)
	go func() {
		defer close(out)
		for t, err := tclient.Recv(); err != nil; t, err = tclient.Recv() {
			out <- t
		}
	}()
	return out, nil
}
