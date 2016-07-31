set terminal png size 1280,720
unset xtics
set logscale x
set key top left
set style line 1 lt 1 lw 3 pt 3 linecolor rgb "red"
set output 'images/latency.png'
plot './xlabels.dat' with labels center offset 0, 1.5 point,\
   "/home/alarmnummer/tmp/2016-07-31__12_20_21/AtomicLongTest-write.hgrm" using 4:1 with lines, \
   "/home/alarmnummer/tmp/2016-07-31__12_20_21/AtomicLongTest-get.hgrm" using 4:1 with lines, \
