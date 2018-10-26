OUTFILE_TITLE = Projektarbeit

all:
	sed -e 's: ,,: \\glqq{}:g' Readme.md > temp.md
	sed -e 's:__OUTFILE_TITLE__:$(OUTFILE_TITLE):g' Readme.tex > temp_template.tex
	# png ist in Markdown gut, aber eps ist in LaTeX besser (use GIMP!). Beide Bilddateien noetig
	sed -i -e 's:\.png):\.eps):g' temp.md
	sed -i -e 's:~~~java:\\begin{minted}{java}:g' temp.md
	sed -i -e 's:~~~cpp:\\begin{minted}{cpp}:g' temp.md
	sed -i -e 's:~~~xml:\\begin{minted}{xml}:g' temp.md
	sed -i -e 's:~~~:\\end{minted}:g' temp.md
	# in diesem Fall sind die Bilder im images-Unterordner. Die LaTeX-Dateien aber auch!
	sed -i -e 's:](images/:](images/:g' temp.md
	pandoc --highlight-style=pygments -f markdown+multiline_tables temp.md -o temp.tex
	# Das ist notwendig, um in LaTeX hinterher gut auf Bilder \ref zu koennen und
	# man keine Markdown Bildangaben nimmt
	sed -i -e 's:{\\label{img\:\([^}]*\)}:\[\1\]{\\label{img\:\1}:g' temp.tex
	latex -shell-escape temp_template.tex
	# sometimes it is better for directory
	latex -shell-escape temp_template.tex
	dvips temp_template
	ps2pdf temp_template.ps $(OUTFILE_TITLE).pdf

clean:
	rm -rf _minted-temp_template
	rm -rf temp*
