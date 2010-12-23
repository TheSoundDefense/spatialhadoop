out_filename = File.expand_path("../res/test.txt", File.dirname(__FILE__))
File.open(out_filename, "w") do |f|
  1024.times do |i|
    x1 = rand(1024)
    y1 = rand(1024)
    x2 = rand(1024)
    y2 = rand(1024)
    str = "%011d,%04d,%04d,%04d,%04d" % [i, x1, y1, x2, y2]
    f.puts str
  end
end
