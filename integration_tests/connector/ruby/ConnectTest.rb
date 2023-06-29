# Requires mysql2 package ('gem install mysql2'). Please check https://github.com/brianmario/mysql2#readme for install guide.

#!/usr/bin/ruby
require 'mysql2'

begin

    client = Mysql2::Client.new(
        host: ARGV[0],
        username: ARGV[1],
        password: ARGV[2],
        port: 4000,
        database: 'test',
        ssl_mode: :verify_ca,
    )

    result = client.query('SHOW DATABASES')
    p result.first

    client.close if client
end