def parseValue(text)
  md = /\d+/.match(text)
  unless md.nil? || (md[0].length != text.length)
    text.to_i
  end 

  md = /\d+\.\d+/.match(text)
  unless md.nil? || (md[0].length != text.length)
    text.to_f
  end 

  if (/true|false/ =~ text)
    return text
  end

  return "\"" + text + "\""
end

def handleTokenizer(tokNode)
  json = "\"tokenizer\" : { "
  fields = tokNode.get_elements("./field")
  if (fields.length > 0)
    json << "\"fields\" : ["
    fields.collect! { |f| "\"" + f.text + "\"" }
    json << fields.join(" , ")
    json << " ]"
  end
  json << " }"
  return json
end

def convertQueries(qNodes)
  conversions = [] 
  qNodes.each { |qn|
    buffer = "{ "  
    qn.children.each_with_index { |c,idx|
      next if (c.class == REXML::Text)
      buffer << "\"" << c.name << "\" : \"" << c.text << "\""
      buffer << ", " if (idx < qn.children.length-2)
    }
    buffer << " }"    
    conversions << buffer
  }
  return conversions
end

require 'rexml/document'
include REXML

filename = ARGV.shift

dom = Document.new(File.open(filename, "r"))
content = File.read(filename)

param = dom.children[0]
if (param.name != "parameters")
  puts "Not a parameters file!"
  exit
end 

newParams = []
param.children.each_with_index { |child, idx|
  next if child.class == REXML::Text
  case child.name
  when "tokenizer"
    newParams << handleTokenizer(child)
  when "query"
    # Skip these - do as an array
  else 
    newParams << "\"#{child.name}\" : #{parseValue(child.text)}"
  end
}
queries = param.get_elements("./query")
tmp = ""
if (queries.length > 0)
  tmp << "\"queries\" : [ "
  tmp << convertQueries(queries).join(",\t\n")
  tmp << " ]"
  newParams << tmp
end
puts "{\n"
puts newParams.join(",\n")
puts "}\n"

