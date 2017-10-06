Gem::Specification.new do |s|
  s.name          = 'logstash-codec-bulk_es'
  s.version       = '0.1.1'
  s.licenses      = ['Apache License (2.0)']
  s.summary       = 'This codec decodes messages received in the Elasticsearch bulk format.'
  s.description   = 'This gem is a logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program'
  s.homepage      = 'http://www.elastic.co/guide/en/logstash/current/index.html'
  s.authors       = ['']
  s.email         = 'saran.ramjeet@gmail.com'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "codec" }

  # Gem dependencies
  s.add_runtime_dependency 'logstash-core-plugin-api', "~> 2.0"
  s.add_runtime_dependency 'logstash-codec-line'

  s.add_development_dependency 'logstash-devutils'
end