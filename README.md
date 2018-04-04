# PG Data Converter

This is a simple Ruby script that can be used in data migration between PostgreSQL databases.

# Dependencies

* libpq headers
* pg gem >= 1.0
* Ruby (tested with 2.5.0, older versions may work)

# Setup

`gem install pg`

or

`bundle install`

# Usage

* Define the data mapping (see `configuration.yml.example`)
* Run `ruby convert.rb`

# Contribute

Issues and pull requests are welcome.