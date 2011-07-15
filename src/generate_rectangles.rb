# USAGE: generate_rectangles <output path> <file size>
if ARGV.empty?
  puts "USAGE: generate_rectangles <output path> <file size>"
end
xmin = 0
xmax = 0x400
ymin = 0
ymax = 0x400
max_width = 0xa
max_height = 0xa

out_filename = ARGV[0] || File.expand_path("../res/test.txt", File.dirname(__FILE__))
# Default size of 1 Gb
total_size = (ARGV[1] && ARGV[1].to_i) || 1024 * 1024 * 1024 * 1
type = (ARGV[2] && ARGV[2].to_i) || 1

current_size = 0
i = 0
File.open(out_filename, "w") do |f|
  while current_size < total_size do
    x1 = rand(xmax-xmin)+xmin
    y1 = rand(ymax-ymin)+ymin
    w = rand([max_width, xmax-x1].min) + 1
    h = rand([max_height, ymax-y1].min) + 1
    str = "%x,%x,%x,%x,%x"%[i, x1, y1, w, h]
    i += 1
    current_size += str.length + 1
    f.puts str
  end
end
