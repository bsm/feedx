# Feedx

[![Build Status](https://travis-ci.org/bsm/feedx.png?branch=master)](https://travis-ci.org/bsm/feedx)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

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
