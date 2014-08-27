Gem::Specification.new do |s|
  s.name        = 'lenz_base'
  s.version     = '0.0.1'
  s.date        = '2014-08-27'
  s.summary     = "Lenz base"
  s.description = "Lenz base"
  s.authors     = ["Seok Heo"]
  s.email       = 'heoseok87@leevi.co.kr'
  s.files       = ["lib/lenz_base.rb"]
  s.license       = 'MIT'
  
  s.add_dependency('json', [">= 0"])
  s.add_dependency('bunny', [">= 1.3.1"])
  s.add_dependency('mysql2', [">= 0.3.0"])
  s.add_dependency('logger', [">= 1.2.8"])
end