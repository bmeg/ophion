
package ophion

import (
	"google.golang.org/grpc"
)


func OphionConnect(address string) (QueryClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
  if err != nil {
    return nil, err
  }
  out := NewQueryClient(conn)
  return out, err
}
