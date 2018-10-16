# Hallo Datas

Das soll eine kleine DXRAM App sein, die mehrere Variablen in einer Hallo-Klasse
in einem DXRAM HalloChunk ablegen soll.

Ziel ist es, mit `HalloChunk.get()` das Objekt `Hallo` zu bekommen. `HalloChunk`
soll als eine Art Wrapper für DXRAM arbeiten, da auch andere Projekte (ohne DXRAM)
`Hallo` verwenden.

Aktuell registiert der erste Peer den `HalloChunk` als "HERE" und füllte den Chunk
mit einem `Hallo` Objekt. Ein 2. Peer sucht nach "HERE" und holt mit `get()` den
Chunk und somit auch das `Hallo` Object.

## Tools Environment

Mit `. ./environment_tools.sh` stehen in der Konsole ein paar Befehle
zur Verfügung, um

- leicht in wichtige Ordner zu wechseln
- nach einem `./build.sh` in `dxapp/` die Application im binary Ordner von DXRAM abzulegen
- zookeeper starten
- DXRAM Superpeer(s) und Peers zu starten

Das Bashskript ist sehr übersichtlich und hier muss man evtl. an 3 Stellen andere
Ordner eintragen. Config Dateien von DXRAM muss man selber anpassen:

**Do not forget to add the app to m_autoStart in $DXRAM_RUN/config/dxram.json**

## Bugs

- derzeit scheint ein 2. Peer den CHunk nicht korrekt holen zu können

