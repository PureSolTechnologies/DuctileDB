import React from 'react';

export default class Header extends React.Component {

    constructor( props ) {
        super( props );
    }

    render() {
        return (
            <div className="row">
                <div className="col-md-12">
                    <h1>Header</h1>
                    <hr />
                </div>
            </div >
        );
    }
}
