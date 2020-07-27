FROM ruby:2.7.1-buster

RUN apt update -y && apt install -y -V ca-certificates lsb-release wget

RUN wget https://apache.bintray.com/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    apt install -y -V ./apache-arrow-archive-keyring-latest-$(lsb_release --codename --short).deb && \
    apt update

RUN apt install -y -V libarrow-glib-dev libparquet-glib-dev

RUN gem install gobject-introspection red-arrow red-parquet

RUN gem install fluentd

RUN mkdir -p /work

COPY . /work/

WORKDIR /work

RUN gem build fluent-plugin-s3.gemspec && gem install fluent-plugin-s3*gem

CMD [ "/bin/bash" ]
