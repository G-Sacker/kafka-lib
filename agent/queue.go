package agent

type Redis interface {
	RPush(string, interface{}) error
	LPop(string, interface{}) error
	IsKeyNotExists(error) bool
}

// queueImpl
type queueImpl struct {
	redis     Redis
	queueName string
}

func (impl *queueImpl) push(v *message) error {
	return impl.redis.RPush(impl.queueName, v)
}

func (impl *queueImpl) pop() (v message, err error) {
	err = impl.redis.LPop(impl.queueName, &v)

	return
}

func (impl *queueImpl) isEmpty(err error) bool {
	return impl.redis.IsKeyNotExists(err)
}
