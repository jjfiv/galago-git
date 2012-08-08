newPrefix = "/work1/allan/jdalton/linked-books/data"
STDIN.each { |l|
  parts = l.strip.split("/")[-4, 4]
  parts[-1].gsub!("mbtei", "mbtei_linked")  
  newFile = File.join(newPrefix, parts)
  if (File.exists?(newFile)) 
    puts newFile
  else
    puts l.strip
  end
}
