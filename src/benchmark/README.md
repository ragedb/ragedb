# Benchmarks

As stated in google benchmark documentation, to disable CPU scaling use cpupower tool.

Install the linux tools generic package:

    sudo apt-get install linux-tools-generic

Check the cpupower frequency information:

    cpupower frequency-info -o proc

If you see this something like this with "powersave" as the governor:

              minimum CPU frequency  -  maximum CPU frequency  -  governor
    CPU  0       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  powersave
    CPU  1       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  powersave
    CPU  2       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  powersave
    CPU  3       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  powersave
    CPU  4       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  powersave
    CPU  5       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  powersave
    CPU  6       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  powersave
    CPU  7       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  powersave

Run this command to use the performance governor instead:

    sudo cpupower frequency-set --governor performance

Check to make sure it took effect:

    cpupower frequency-info -o proc

          minimum CPU frequency  -  maximum CPU frequency  -  governor
    CPU  0       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  performance
    CPU  1       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  performance
    CPU  2       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  performance
    CPU  3       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  performance
    CPU  4       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  performance
    CPU  5       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  performance
    CPU  6       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  performance
    CPU  7       800000 kHz ( 15 %)  -    5100000 kHz (100 %)  -  performance

Now you can run the benchmarks. Once you are done, reset it back to powersave

    sudo cpupower frequency-set --governor powersave
    