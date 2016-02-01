# More info: https://www.githubarchive.org/

require 'big_query'

def format_result(result)
  schema = result['schema']['fields']
  result['rows'].reduce({}) do |accum, row|
    row['f'].each_with_index do |f,i|
      accum[schema[i]['name']] = f['v']
    end
    accum
  end
end

def query_archive(query)
  config = {
    'client_id'     => '',
    'service_email' => '',
    'key'           => 'google_api_key.p12',
    'project_id'    => 'winged-plate-867',
    'dataset'       => 'githubarchive:day'
  }

  bq = BigQuery::Client.new(config)
  result = bq.query(query)
  format_result(result)
end

namespace :gh do
  desc "Find PRs opened on a given date, eg: rake gh:repos[20150101]"
  task :prs_opened, [:date]  do |task, args|
    args.with_defaults(date: Time.new().strftime('%Y%m%d'))

    query = "SELECT COUNT(*) AS count FROM(SELECT repo.name, JSON_EXTRACT(payload, '$.action') as action FROM [githubarchive:day.events_#{args.date}] WHERE type = 'PullRequestEvent') WHERE action CONTAINS 'opened'"
    results = query_archive(query)

    p [:results, results]
  end

  desc "Find PRs opened over Jan 2016 weekends"
  task :prs_opened_jan do |task, args|
    weekends = %w(20160109 20160110 20160116 20160117 20160123 20160124 20160130 20160131)

    puts "DATE COUNT"
    weekends.each {|date|
      query = "SELECT COUNT(*) AS count FROM(SELECT repo.name, JSON_EXTRACT(payload, '$.action') as action FROM [githubarchive:day.events_#{date}] WHERE type = 'PullRequestEvent') WHERE action CONTAINS 'opened'"
      results = query_archive(query)
      puts "%3d %5d %s\n" % [date, results['count'], "#" * (results['count'].to_i / 300)]
    }
  end

end
