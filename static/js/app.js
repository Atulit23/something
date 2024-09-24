function fetchData(endpoint) {
    $.ajax({
        url: endpoint,
        method: 'GET',
        success: function(data) {
            let preId = '';
            switch (endpoint) {
                case '/count_movies_tvshows_by_director':
                    preId = '#result1';
                    break;
                case '/country_with_highest_comedy_movies':
                    preId = '#result2';
                    break;
                case '/max_movies_by_director_per_year':
                    preId = '#result3';
                    break;
                case '/average_duration_per_genre':
                    preId = '#result4';
                    break;
                case '/directors_horror_comedy_movies':
                    preId = '#result5';
                    break;
            }
            $(preId).text(JSON.stringify(data, null, 2));
        },
        error: function(error) {
            alert('Error fetching data: ' + error.responseText);
        }
    });
}
