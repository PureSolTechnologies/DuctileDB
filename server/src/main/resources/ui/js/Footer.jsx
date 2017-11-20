import React from 'react';

import PureSolTechnologies from './components/PureSolTechnologies';

export default class Footer extends React.Component {

    constructor( props ) {
        super( props );
    }

    render() {
        return (
            <div className="row">
                <div className="col-md-12">
                    <hr />
                    <PureSolTechnologies />
                </div>
            </div >
        );
    }
}
