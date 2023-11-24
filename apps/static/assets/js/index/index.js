/*
Copernicus Operations Dashboard

Copyright (C) ${startYear}-${currentYear} ${Telespazio}
All rights reserved.

This document discloses subject matter in which TPZ has
proprietary rights. Recipient of the document shall not duplicate, use or
disclose in whole or in part, information contained herein except for or on
behalf of TPZ to fulfill the purpose for which the document was
delivered to him.
*/

class Home {

  constructor(){

  }

  init(){

    // Hide scrollbars in Home Page
    $('body,html').css('overflow-x','hidden');

    // Hide the time period selector combobox
    $('#time-period-select-container').hide();

    // Hide footer
    $('.footer').hide();

    // Shows logos in the Nav header
    $('#copernicus-logo-header').show();
    $('#ec-logo-header').show();

    // Load the events from local db
    console.info('Loading events...');
    asyncAjaxCall('/api/events/anomalies/last-24h', 'GET', {}, this.succesLoadAnomalies, this.errorLoadAnomalies);

    // Load data takes and calculate the hours of sensing
    console.info('Retrieving the hours of sensing...');
    this.displaySensingTimeMinutes();

    // Retrieve the number and size of the published products
    console.info('Retrieving the volume and number of published products...');
    this.displayPublishedProductsVolumeCount();

    // Remove Home video controls
    $('#home-video').hover(function toggleControls() {
        if (this.hasAttribute("controls")) {
            this.removeAttribute("controls")
        } else {
            this.setAttribute("controls", "controls")
        }
    })

    // Play video only after page loading
    window.onload = function () {
        $('#home-video').get(0).play();
    }

    return;
  }

  succesLoadAnomalies(response){

    // Format the response from the query
    var rows = format_response(response);
    console.info('Events loaded. Num of events: ' + rows.length);

    // Define the events
    var now = new Date();
    var count = 0;
    var details = [];
    for (var i = 0 ; i < rows.length ; ++i) {
        var element = rows[i];

        // Until a full parsing of news is implemented, the start time is based
        // on the publication date, and the end time is set as 1 hour later
        var start_time = moment(element['start'], 'DD/MM/YYYY HH:mm:ss').toDate();
        var end_time = moment(element['end'], 'DD/MM/YYYY HH:mm:ss').toDate();

        // Append the anomaly in the list of items to be displayed if:
        // 1. The anomaly occurred within 48h from now
        // 2. There is at least one impacted datatake
        if ((now.getTime() - start_time.getTime() <= 48 * 60 * 60 * 1000) &&
                home.datatakesImpacted(element)) {

            // Increment count
            count++;

            // Save the event details
            // Create a new event to add in the Timeline
            // Modify the end date, based on the issue type
            var title;
            var category = element["category"];
            var item = element["impactedSatellite"];
            if (category == 'Platform'){
                title = 'Satellite issue, affecting ' + item + ' data.';
            } else if (category == 'Acquisition'){
                title = 'Acquisition issue, affecting ' + item + ' data.';
                end_time.setTime(start_time.getTime() + 15 * 60 * 1000);
            } else if (category == 'Production'){
                title = 'Production issue, affecting ' + item + ' data.';
                end_time.setTime(start_time.getTime() + 120 * 60 * 1000);
            } else {
                title = 'Data Access issue, affecting ' + item + ' data.';
                end_time.setTime(start_time.getTime() + 15 * 60 * 1000);
            }

            // Add link to timeline session
            title += ' <a href="/events.html?showDayEvents=' + element['publicationDate'].substring(0, 10) + '">Read More</a>';

            // Append event details in the details panel
            console.info('Appending event: ' + title);
            details.push({
                'time' : start_time,
                'content' : title});

            // Limit the events count to 3 occurrences
            if (count == 2) break ;
        }
    }

    // If no events is present, display a pleasant statement
    if (details.length == 0) {
        console.info('No recent event collected');
        $('#real-time-event-list').append(
            '<li class="feed-item feed-item-success">' +
                '<i class="flaticon-success feed-item-icon"></i>' +
                '<h4 style="color: white; position: relative; top: -27px" class="text">Nominal operations</h4>' +
            '</li>'
        );
        return
    }

    // Otherwise, display the collected events
    $('#real-time-event-list').html('');
    for (var i = 0; i < details.length; ++i) {
        var time_ago = (now.getTime() - details[i].time.getTime()) / (3600000);
        var time_measures = ' hour(s) ago';
        if (time_ago > 24) {
            let hours = time_ago - 24;
            time_ago = '1 day, ' + Math.round(hours);
        } else if (time_ago > 1) {
            time_ago = Math.round(time_ago);
        } else {
            time_ago = (now.getTime() - details[i].time.getTime()) / (60000);
            time_measures = ' minute(s) ago';
        }
        $('#real-time-event-list').append(
            '<li class="feed-item feed-item-warning" style="color: white">' +
                '<i class="flaticon-alarm-1 feed-item-icon"></i>' +
                '<time class="date">' + time_ago + time_measures + '</time>' +
                '<div class="text" style="word-wrap: break-word; max-width: 250px">' + details[i].content + '</div>' +
            '</li>'
        );
    }

    return;
  }

  errorLoadAnomalies(response){
    return;
  }

  datatakesImpacted(anomaly){

    console.info('Checking datatake impact for anomaly: ' + anomaly["category"]);

    // Analyze the impact on production, anc choose the proper colour. If all products associated to
    // data takes where restored, display the anomaly in green; otherwise, use default orange color.
    var threshold = 90;
    var datatakes_completeness = format_response(anomaly['datatakes_completeness']);
    var completeness = 0;
    var allRecovered = true;
    for (var index = 0; index < datatakes_completeness.length; ++index) {
        try {
            for (const [key, value] of Object.entries(JSON.parse(datatakes_completeness[index].replaceAll('\'','\"')))) {
                var objValues = Object.values(value);
                completeness = (objValues[1] + objValues[2] + objValues[3]) / 3;
            }
            if (completeness < threshold) {
                allRecovered = false;
                break ;
            }
        } catch (ex) {
            console.warn('An error occurred, while parsing the product level count string');
            console.warn("Error ", ex);
            console.warn(anomaly);
        }
    }
    return !allRecovered;
  }

  displaySensingTimeMinutes(){
    datatakes.calcSensingTime24H();
    (async() => {
        while(datatakes.sensingTime24H == 0)
            await new Promise(resolve => setTimeout(resolve, 250));
        console.info('Minutes of sensing: ' + datatakes.sensingTime24H * 60);
        $('#sensing-time-minutes').html(Math.round(datatakes.sensingTime24H * 60));
    })();
  }

  displayPublishedProductsVolumeCount(){
    publicdata.get_published_count_size_last_24h();
    (async() => {
        while(!publicdata.published_last24h['NUM'])
            await new Promise(resolve => setTimeout(resolve, 250));
        console.info('Number of published products: ' + publicdata.published_last24h['NUM']);
        $('#published-products-count').html(new Intl.NumberFormat().format(publicdata.published_last24h['NUM']));
    })();
    (async() => {
        while(!publicdata.published_last24h['VOL'])
            await new Promise(resolve => setTimeout(resolve, 250));
            var vol = publicdata.published_last24h['VOL'] / (1024 * 1024 * 1024 * 1024);
        console.info('Volume of published products: ' + vol);
        $('#published-products-volume').html(new Intl.NumberFormat().format(vol.toFixed(2)));
    })();
  }
}

let home = new Home();