# NOW := $(shell date +"%c" | tr ' :' '__')
#NOW := $(shell date +"%Y%m%d-%H%M")
OUTFILE_TITLE = HBase auf DXRAM 
OUTFILE_SUBTITLE = Einstiegspunkte für DXRAM in Hadoop \\\& HBase
OUTFILE_AUTHOR = in Bearbeitung\!
OUTFILE_INSTITUTE = Department of Computer Science \\\\ Heinrich-Heine-University Düsseldorf, Germany
#OUTFILE = ./Slides.$(AUTHOR).$(NOW).pdf
OUTFILE = Slides.pdf

all:
	bash svg2pdf.sh
	sed -e 's:__OUTFILE_TITLE__:$(OUTFILE_TITLE):g' Readme.tex > temp2_Readme.tex
	sed -e 's:__OUTFILE_AUTHOR__:$(OUTFILE_AUTHOR):g' temp2_Readme.tex > temp_Readme.tex
	sed -e 's:__OUTFILE_SUBTITLE__:$(OUTFILE_SUBTITLE):g' temp_Readme.tex > temp2_Readme.tex
	sed -e 's:__OUTFILE_INSTITUTE__:$(OUTFILE_INSTITUTE):g' temp2_Readme.tex > temp_Readme.tex
	sed -e 's: ,,: \\glqq{}:g' Readme.md > temp.md
	# png ist in Markdown gut, aber pdf ist in LaTeX besser
	sed -i -e 's:\.png):\.pdf):g' temp.md
	# in diesem Fall sind die Bilder im fig-Unterordner. Die LaTeX-Dateien aber auch!
	sed -i -e 's:](fig/:](fig/:g' temp.md
	# terrible c++ hightlightning hack
	sed -i -e 's:~~~java:\\begin{minted}{java}:g' temp.md
	sed -i -e 's:~~~cpp:\\begin{minted}{cpp}:g' temp.md
	# xml not realy working with pandoc and minted !!!
	sed -i -e 's:~~~xml:\\begin{minted}{xml}:g' temp.md
	sed -i -e 's:~~~bash:\\begin{minted}{bash}:g' temp.md
	sed -i -e 's:~~~:\\end{minted}:g' temp.md
	# pandoc :-D
	pandoc --highlight-style=pygments -f markdown+multiline_tables --slide-level 2 -t beamer temp.md -o temp.tex
	pdflatex -shell-escape temp_Readme.tex
	mv temp_Readme.pdf $(OUTFILE)

tex:
	pdflatex -shell-escape temp_Readme.tex
	mv temp_Readme.pdf $(OUTFILE)

clean:
	rm -rf _minted-temp_Readme
	rm -rf temp*
