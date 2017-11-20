import React from 'react';
import ReactDOM from 'react-dom';
import { Provider } from 'react-redux';
import { Router, Route, Redirect, IndexRoute, browserHistory } from 'react-router';

import Layout from './Layout';
import store from './flux/Store';
import LandingPage from './pages/LandingPage';
import Metrics from './pages/Metrics';
import Browser from './pages/Browser';

ReactDOM.render(
    <Provider store={store}>
        <Router history={browserHistory}>
            <Route path="/" component={Layout}>
                <IndexRoute component={LandingPage} />
                <Route path="metrics" component={Metrics} />
                <Route path="browser" component={Browser} />
            </Route>
        </Router>
    </Provider >,
    document.getElementById( 'app' )
);
