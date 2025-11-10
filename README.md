# Feedx

[![Go reference](https://pkg.go.dev/badge/github.com/bsm/feedx.svg)](https://pkg.go.dev/github.com/bsm/feedx)
[![GitHub release](https://img.shields.io/github/tag/bsm/feedx.svg?label=release)](https://github.com/bsm/feedx/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/bsm/feedx)](https://goreportcard.com/report/github.com/bsm/feedx)[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Feed-based data exchange between services.

## Usage (Ruby)

```ruby
require 'bfs/s3'
require 'feedx'

# Init a new producer with an S3 destination
relation = Post.includes(:author)
producer = Feedx::Producer.new relation, 's3://my-bucket/feeds/users.json.gz'

# Push a new feed every hour
loop do
  producer.perform
  sleep(3600)
end
```
