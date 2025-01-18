package main;

import lombok.extern.slf4j.Slf4j;

import options.WhatIfTablesOptions;

import pipelines.WhatIfTablesPipeline;

@Slf4j
public class WhatIfDataflowDriver {

    public static void main(String[] args) {

        WhatIfTablesOptions whatIfTablesOptions = WhatIfTablesOptions.fromArgs(args);

        log.info("The pipeline is starting.");

        WhatIfTablesPipeline.of(whatIfTablesOptions);

        log.info("The pipeline has finished.");
    }
}