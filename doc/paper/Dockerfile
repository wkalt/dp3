FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y texlive \
texlive-pictures texlive-latex-extra texlive-bibtex-extra \
biber build-essential

WORKDIR /data

CMD ["make", "build"]
