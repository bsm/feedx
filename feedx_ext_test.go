package feedx

func (c *consumer) TestSync() error {
	_, err := c.sync(false)
	return err
}
