package feedx

import "time"

func (c *consumer) TestSync() error {
	_, err := c.sync(false)
	return err
}

func TimestampFromTime(t time.Time) timestamp {
	return timestampFromTime(t)
}
