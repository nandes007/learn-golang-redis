package learn_redis

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

var client = redis.NewClient(&redis.Options{
	Addr:    "localhost:6379",
	DB:      0,
	Network: "tcp4",
})

func TestConnection(t *testing.T) {
	assert.NotNil(t, client)

	// err := client.Close()
	// assert.Nil(t, err)
}

var ctx = context.Background()

func TestPing(t *testing.T) {
	result, err := client.Ping(ctx).Result()
	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	client.SetEx(ctx, "name", "Steven Gilbert Simanjuntak", 3*time.Second)

	result, err := client.Get(ctx, "name").Result()
	assert.Nil(t, err)
	assert.Equal(t, "Steven Gilbert Simanjuntak", result)

	time.Sleep(5 * time.Second)

	result, err = client.Get(ctx, "name").Result()
	assert.NotNil(t, err)
}

func TestList(t *testing.T) {
	client.RPush(ctx, "names", "Fernandes")
	client.RPush(ctx, "names", "Ariadi")
	client.RPush(ctx, "names", "Simanjuntak")

	assert.Equal(t, "Fernandes", client.LPop(ctx, "names").Val())
	assert.Equal(t, "Ariadi", client.LPop(ctx, "names").Val())
	assert.Equal(t, "Simanjuntak", client.LPop(ctx, "names").Val())

	client.Del(ctx, "names")
}

func TestSet(t *testing.T) {
	client.SAdd(ctx, "students", "Fernandes")
	client.SAdd(ctx, "students", "Fernandes")
	client.SAdd(ctx, "students", "Ariadi")
	client.SAdd(ctx, "students", "Ariadi")
	client.SAdd(ctx, "students", "Simanjuntak")
	client.SAdd(ctx, "students", "Simanjuntak")

	assert.Equal(t, int64(3), client.SCard(ctx, "students").Val())
	assert.Equal(t, []string{"Ariadi", "Fernandes", "Simanjuntak"}, client.SMembers(ctx, "students").Val())
}

func TestSortedSet(t *testing.T) {
	client.ZAdd(ctx, "scores", redis.Z{Score: 100, Member: "Nandes"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 85, Member: "Andreas"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 95, Member: "Steven"})

	assert.Equal(t, []string{"Andreas", "Steven", "Nandes"}, client.ZRange(ctx, "scores", 0, 2).Val())
	assert.Equal(t, "Nandes", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Steven", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Andreas", client.ZPopMax(ctx, "scores").Val()[0].Member)
}

func TestHash(t *testing.T) {
	client.HSet(ctx, "user:1", "id", "1")
	client.HSet(ctx, "user:1", "name", "Nandes")
	client.HSet(ctx, "user:1", "email", "nandes@example.com")

	user := client.HGetAll(ctx, "user:1").Val()
	assert.Equal(t, "1", user["id"])
	assert.Equal(t, "Nandes", user["name"])
	assert.Equal(t, "nandes@example.com", user["email"])

	client.Del(ctx, "user:1")
}

func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko A",
		Longitude: 106.818489,
		Latitude:  -6.178966,
	})
	client.GeoAdd(ctx, "sellers", &redis.GeoLocation{
		Name:      "Toko B",
		Longitude: 106.821568,
		Latitude:  -6.180662,
	})

	distance := client.GeoDist(ctx, "sellers", "Toko A", "Toko B", "km").Val()
	assert.Equal(t, 0.3892, distance)

	sellers := client.GeoSearch(ctx, "sellers", &redis.GeoSearchQuery{
		Longitude:  106.819143,
		Latitude:   -6.108182,
		Radius:     10,
		RadiusUnit: "km",
	}).Val()

	assert.Equal(t, []string{"Toko A", "Toko B"}, sellers)
}

func TestHyperLogLog(t *testing.T) {
	client.PFAdd(ctx, "visitors", "nandes", "simanjuntak")
	client.PFAdd(ctx, "visitors", "andreas", "ferdinan")
	client.PFAdd(ctx, "visitors", "steven", "gilbert")
	assert.Equal(t, int64(6), client.PFCount(ctx, "visitors").Val())
}

func TestPipeline(t *testing.T) {
	client.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.SetEx(ctx, "name", "Nandes", time.Second*5)
		p.SetEx(ctx, "address", "Indonesia", time.Second*5)
		return nil
	})

	assert.Equal(t, "Nandes", client.Get(ctx, "name").Val())
	assert.Equal(t, "Indonesia", client.Get(ctx, "address").Val())
}

func TestTransaction(t *testing.T) {
	_, err := client.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.SetEx(ctx, "name", "Steven", time.Second*5)
		p.SetEx(ctx, "address", "Medan", time.Second*5)
		return nil
	})

	assert.Nil(t, err)

	assert.Equal(t, "Steven", client.Get(ctx, "name").Val())
	assert.Equal(t, "Medan", client.Get(ctx, "address").Val())
}

func TestPublishStream(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: "members",
			Values: map[string]interface{}{
				"name":    "Nandes",
				"address": "Indonesia",
			},
		}).Err()

		assert.Nil(t, err)
	}
}

func TestCreateConsumerGroup(t *testing.T) {
	client.XGroupCreate(ctx, "members", "group-1", "0")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-1")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-2")
}

func TestConsumeStream(t *testing.T) {
	streams := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "group-1",
		Consumer: "consumer-1",
		Streams:  []string{"members", ">"},
		Count:    2,
		Block:    5 * time.Second,
	}).Val()

	fmt.Println(len(streams))

	for _, stream := range streams {
		for _, message := range stream.Messages {
			fmt.Println(message.ID)
			fmt.Println(message.Values)
		}
	}
}

func TestSubscribePubSub(t *testing.T) {
	subscriber := client.Subscribe(ctx, "channel-1")
	defer subscriber.Close()
	for i := 0; i < 10; i++ {
		message, err := subscriber.ReceiveMessage(ctx)
		assert.Nil(t, err)
		fmt.Println(message.Payload)
	}
}

func TestPublishPubSub(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.Publish(ctx, "channel-1", "Hello "+strconv.Itoa(i)).Err()
		assert.Nil(t, err)
	}
}
