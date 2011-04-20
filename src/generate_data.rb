xmin = 0
xmax = 1024
ymin = 0
ymax = 1024
max_width = 10
max_height = 10

out_filename = File.expand_path(ARGV[0] || "../res/test.txt", File.dirname(__FILE__))
# Default size of 1 Gb
total_size = (ARGV[1] && ARGV[1].to_i) || 1024 * 1024 * 1024 * 1

current_size = 0
i = 0
File.open(out_filename, "w") do |f|
  while current_size < total_size do
    x1 = rand(xmax-xmin)+xmin
    y1 = rand(ymax-ymin)+ymin
    w = rand(max_width)
    h = rand(max_height)
    str = [i, x1, y1, x1+w, y1+h].join(",")
    i += 1
    current_size += str.length + 1
    f.puts str
  end
end
