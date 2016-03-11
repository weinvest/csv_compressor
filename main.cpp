#include <iostream>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include "ColumnCompressor.h"

int main(int argc, char** argv)
{
    using namespace boost::program_options;
    options_description opts("gcsv options");
    opts.add_options()
            ("help,h","print this help information.")
            ("compress,c",value<bool>()->default_value(true),"is compress?")
            ("input,i",value<std::string>(),"input file")
            ("output,o",value<std::string>(),"output file")
            ("memory,m",value<size_t>()->default_value(1<<24),"cache memory size")
            ("delimiter,d",value<char>()->default_value(','),"delimiter");

    variables_map vm;
    store(parse_command_line(argc, argv, opts),vm);

    if(vm.count("help"))
    {
        std::cout << opts;
        return 0;
    }

    if(0 == vm.count("input"))
    {
        std::cout << "no input file, use -i option to specify." << std::endl;
        std::cout << opts;
        return -1;
    }

    if(0 == vm.count("output"))
    {
        std::cout << "no output file, use -o option to specify." << std::endl;
        std::cout << opts;
        return -1;
    }

    namespace bfs = boost::filesystem;
    std::string inputFilePath(vm["input"].as<std::string>());
    std::string outputFilePath(vm["output"].as<std::string>());

    if(!bfs::exists(inputFilePath))
    {
        std::cout << inputFilePath << " not exists" << std::endl;
        return -1;
    }

    if(bfs::exists(outputFilePath))
    {
        std::cout << outputFilePath << " already exists, may be override it." << std::endl;
        return -1;
    }

    bool isCompress = vm["compress"].as<bool>();
    if(isCompress)
    {
        ColumnCompressor c(vm["delimiter"].as<char>(), vm["memory"].as<size_t>());
        c.Compress(inputFilePath, outputFilePath);
    }
    else
    {

    }

    return 0;
}

