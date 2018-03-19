# Workaround

Ziel diese Workaround ist, auf github einen eigenen *master* Branch
zu haben (`no-go/dxram.git`), in dem man seine Änderungen einpflegt. Man
möchte aber auch da drin immer die Änderungen vom *master*
Branch des `hhu-bsinfo/dxram.git` Repositories haben. Hierzu legt man
sich einen Branch an mit z.B. dem Namen *shared_master*, den man
bei sich nie verändert. Auf seinem PC bindet man diesen Branch
an `hhu-bsinfo/dxram.git`. Hierzu fügt man dieses remote Repository mit dem
Namen z.B. *upstream* dem Branch *shared_master* hinzu. Mit `fetch` holt
man sich dann immer die Änderungen von `hhu-bsinfo/dxram.git`.

Arbeitet man nun mit seinem master branch, macht adds, commits, pushs usw.,
will man irgendwann dann auch mal den Code bzw. seine Änderungen auf
die neuen Sachen von `hhu-bsinfo/dxram.git` anwenden, auch wenn man es
nicht dorthin "pushen" kann. Die ganzen git-history-change Sachen lasse
ich mal weg. Die wäre dafür da, um die vielen kleinen Commits, die man
in seinen Master gemacht hat, zu einem Commit zusammenfassen kann, so dass
man beim Zusammenführen der Sachen aus *master* `hhu-bsinfo/dxram.git`
(eigentlich nur der eigene Branch *shared_master*) 
in den eigenen *master* `no-go/dxram.git` ein wenig Ordnung bringt.

Als erstes macht man auf der Webseite von https://github.com/hhu-bsinfo/dxram
einen fork (dafür muss man eingeloggt sein). So entsteht
die Kopie https://github.com/no-go/dxram .

Nun zieht man sich das mit clone auf den eigenen Rechner:

    git clone git@github.com:no-go/dxram.git

Und geht in den neuen Ordner:

    cd dxram/

Man macht sich nun einen Branch, den man an das gesharte Original `hhu-bsinfo/dxram.git`
binden will:

    git branch shared_master

Nun muss man noch in den Branch wechseln:

    git checkout shared_master

Mit dem Befehl `remote add` wird `hhu-bsinfo/dxram.git` mit dem Name "upstream" an
den aktuellen (nur?) Branch gebunden:

    git remote add upstream git@github.com:hhu-bsinfo/dxram.git

Nun holt man sich die aktuellen Änderungen von "upstream" in den Branch *shared_master* hinein:

    git fetch upstream

Da man in *shared_master* nie lokale Änderungen macht, sollte es hier keinen
Ärger geben. *shared_master* ist nun Aktuell.

Nun macht meine seine Änderungen, wozu man *shared_master* verlässt und in diesem
Workaround auf *master* wechselt:

    git checkout master

Änderungen machen:

    vim build.sh

Änderungen an sein "staging" übergeben:

    git add build.sh

Änderungen an sein lokales Repository auf dem eingen PC committen:

    git commit -m "change build.sh to debug"

... arbeiten, push machen usw. Man arbeiten in seinem *master*.

Nun will man irgendwann mit den Änderungen aus `hhu-bsinfo/dxram.git` weiterarbeiten,
weil es wichtige Änderungen gab oder ähnliches. Dazu wechselt man erstmal
zu *shared_master*:

    git checkout shared_master

Nun holt man sich nochmal die Änderungen:

    git fetch upstream

Von den Änderungen will man nun aus dem *master* alles in den aktuellen
Branch *shared_master* eingepflegt bekommen:

    git rebase upstream/master

Der Branch *shared_master* sollte nun aktuell sein. Um dies in seinen
eigenen geänderten Master ein zu pflegen, wechselt man in seinen eigenen *master*: 

    git checkout master

Mit dem folgenden Befehl werden nun alle Änderungen, die man in
seinem *master* gemacht hat wie ein "patch" behandelt. Dein *master*
wird eine Kopie von *shared_master* und dann werden dein "patch" dort
eingespielt:

    git rebase shared_master

Wenn alles glatt ging, kannst du diese Änderung auf dein eigenes remote
Repository (hier: `no-go/dxram.git`) pushen:

    git push

Wenn man will, kann man auch das *shared_master* nach github pushen:

    git checkout shared_master
    git push --set-upstream origin shared_master

Hinterher nicht vergessen, wieder auf sein *master* zu wechseln, bevor man
versehentlich doch Änderungen in *shared_master* macht:

    git checkout master
