# PgColumnBytePacker

tl;dr: Modifies ActiveRecord's table creation tree walker to automatically re-order columns by alignment size to optimize on disk layout. The general idea and relevant PostgreSQL internals are described in [On Rocks and Sand](https://www.2ndquadrant.com/en/blog/on-rocks-and-sand/).

Note: Because you need the full table definition to re-order columns, the most benefit occurs when the full table is created in one step (rather than added onto with repeated `add_column` migrations).

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'pg_column_byte_packer'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install pg_column_byte_packer

## Usage


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `bundle exec rspec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment. This project uses Appraisal to test against multiple versions of ActiveRecord; you can run the tests against all supported version with `bundle exec appraisal rspec`.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## License

The gem is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
