# sscom-tracker
Tracking Latvian classified/ad portal ss.com for interesting classifieds

## What it is

This python script can be run from `cron` to monitor the classifieds portal ss.com for interesting classifieds.
Classifieds come an go and it seems like a good idea to automate the monitoring of the classifieds portal.

## Setting it up

Clone this repo. Make sure to either use the provided Docker files or install dependencies manually with:

```pip3 install requests lxml python-pushover```

Edit `settings.json`. Use the provided example settings file.
You need to change two things, first what classifieds you'd like to monitor. Currently houses and apartments are tested and supported.

The URL you use is same URL as you type in the browser, so you can kind of browse around, see what works for you and then put it into settings.
In addition to URL, the apartment search also supports filtering by room count using the setting `filter_room_count`.
This will look for apartments with room count `>=` the one you specified.

If you'd like to receive [Pushover](https://pushover.net) push notifications, you need to set `pushover-enabled` to `True` and provide your user key and API token.

You deploy it to a box that is always on and add it to `cron`.
```10 10 * * * cd  /where/you/cloned/it/sscom-tracker && python3 tracker.py > sscom.log```

Of course set the candence to a frequency that suits you.
## Flow

* Download RSS data
* Process and create objects
* Retrieve per classified data
* Enrich classifieds with the data from the descriptions

## Kubernetes

Create a configmap using the settings file, like so:

```
kubectl create configmap pytrackersettings --from-file=settings.json
```

### Known issues

With latest pylint and prospector there is a bug, covered [here](https://github.com/PyCQA/prospector/issues/393).
The suggested workaround in the github issue to comment out a line in `pylint` code works.

## Cache

The original idea was to make it bigger and to make a nice frontend for it.
One of the things that I have added is caching so that I don't do needless trips to the classifieds portal.
Currently the cache is just pickled, later one, some fancier storage could be used for it.

## Testing

### Tests

It currently has about `35%` test coverage.
Tests are written with `pytest` and you can run them with:

```pytest -v tests/* --cov-report term-missing --cov='lib/' --cov='./tracker.py' -v```

### Docker

Bunch of docker files are provided in `docker` directory.
You can build an environment and fire all the tests from within `docker` directory with:

```
docker build -f Dockerfile.sscom-tracker-base -t sscom-tracker-base --no-cache ../
docker build -f Dockerfile.sscom-tracker-testing -t sscom-tracker-testing --no-cache ..
docker run -ti sscom-tracker-testing ./testing.sh
```


### Flow chart
graph TD
    A[Tracker initiated] -->|Initalise all stuff| B(Initialise)
    B --> C(Set up logging)
    B --> D(Load settings)

    D --> E(Load cache)

    E --> E1(Object cache)
    E --> E2(Data cache)

    D --> H(Initialise Retriever)
    E2 --> H(Initialise Retriever)

    H --> F(Load filter)
    D --> F(Load filter)
    E1 --> F(Load filter)

    F --> K{Is the cache fresh?}
    K -->|Yes| L1(Get results from filter)
    K -->|No| L2(Update data cache)

    L1 -->Results
    Results:::gray -->P1{Print?}
    P1:::green -->|Yes| PA(Print results)
    PA(Print results):::orange

    Results:::orange -->P2{Push?}
    P2:::red-->|Yes| PN(Send push notification)


    linkStyle 0 stroke:#aaa,stroke-width:2px,stroke-dasharray: 8 4;

    classDef green fill:#52BE80,stroke:#196F3D;
    classDef red fill:#E74C3C,stroke:#922B21;
    classDef orange fill:#E67E22,stroke:#AF601A;
    classDef gray fill:#D5DBDB,stroke:#85929E,stroke-dasharray: 8 4;
