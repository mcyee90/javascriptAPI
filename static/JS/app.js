// Create a callback function that gets data from /samples/<sampleItem>, /otu, and /metadata/<sampleItem>
function getData(sampleItem , callback) {
    Plotly.d3.json(`/samples/${sampleItem}`, function(error, sample) {
        if (error) return console.warn(error);
        Plotly.d3.json("/otu", function(error, OTU) {
            if (error) return console.warn(error);
            callback(sample, OTU)
        });
    });
    Plotly.d3.json(`/metadata/${sampleItem}`, function(error, response) {
        if (error) return console.warn(error);
        changeMetaData(response);
    });
}

// Create function that will produce a dropdown with samples to choose from
function dropDown() {
    // Use list of sample names to render the select options
    Plotly.d3.json("/names", function (error, response) {
        if (error) return console.warn(error);

        let selection = document.getElementById("select-dataset");
        for (let i = 0; i < response.length; i++) {
            let option = document.createElement("option");
            option.text = response[i];
            option.value = response[i];
            selection.appendChild(option);
        }
        getData(response[0], createCharts);
    });
}

// Create function to initialize dashboard
function init_page() {
    dropDown();
}

// Create function to retrieve new data every time a new sample is selected
function optionChanged(newSample) {
    getData(newSample, createCharts);
}

//Create function to build pie and bubble charts
function createCharts(sample, OTU) {
    // Use map to filter through sample data to find OTU taxonomic name
    let taxonomicName = sample[0]["otu_ids"].map(name => OTU[name]);
    let chartValues = sample[0]["sample_values"].slice(0, 10);
    let chartLabels = sample[0]["otu_ids"].slice(0, 10);
    // Create pie chart
    let pieChart = {
        values: chartValues,
        labels: chartLabels,
        hovertext: taxonomicName.slice(0, 10),
        hoverinfo: "hovertext",
        type: "pie"
    };
    let pieData = [pieChart];
    let pieLayout = {
        margin: {
            t: 0,
            l: 0
        }
    };
    let PIE = document.getElementById("pie");
        Plotly.newPlot(PIE, pieData, pieLayout);

    // Create bubble chart
    let bubbleChart = {
        x: sample[0]["otu_ids"],
        y: sample[0]["sample_values"],
        text: taxonomicName,
        mode: "markers",
        marker: {
            size: sample[0]["sample_values"],
            color: sample[0]["otu_ids"],
            colorscale: "Earth"
        }
    };
    let bubbleData = [bubbleChart]
    let bubbleLayout = {
        margin: {
            t: 0
        },
        hovermode: "closest",
        xaxis: {
            title: "OTU ID"
        }
    };
    let BUBBLE = document.getElementById("bubble");
    Plotly.newPlot(BUBBLE, bubbleData, bubbleLayout);
}

// Create function to change metadata info
function changeMetaData(data) {
    let $panelBody = document.getElementById("metadata-sample");
    // clear any existing metadata
    $panelBody.innerHTML = "";
    // Loop through keys in json response and create new tags for metadata
    for (let key in data) {
    h5Tag = document.createElement("h5");
    metadataText = document.createTextNode(`${key}: ${data[key]}`);
    h5Tag.append(metadataText);
    $panelBody.appendChild(h5Tag);
    }
}


init_page();
