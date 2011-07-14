# USAGE: generate_rectangles <output path> <file size>
if ARGV.empty?
  puts "USAGE: generate_rectangles <output path> <file size>"
end
xmin = 0
xmax = 1024
ymin = 0
ymax = 1024

out_filename = File.expand_path(ARGV[0] || "../res/test.txt", File.dirname(__FILE__))
# Default size of 1 Gb
total_size = (ARGV[1] && ARGV[1].to_i) || 1024 * 1024 * 1024 * 1

current_size = 0
i = 1
File.open(out_filename, "w") do |f|
  while current_size < total_size do
    x = rand(xmax-xmin)+xmin
    y = rand(ymax-ymin)+ymin
    type = rand(types)
    str = [i, x, y].join(",")
    i += 1
    current_size += str.length + 1
    f.puts str
  end
end
