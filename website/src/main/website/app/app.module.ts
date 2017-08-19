import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { FormsModule } from '@angular/forms';
import { AppComponent } from './app.component';

import {PureSolTechnologiesComponent} from './commons/puresol-technologies.component';
import {PurifinityComponent} from './commons/purifinity.component';

@NgModule( {
    imports: [
        BrowserModule,
        FormsModule
    ],
    declarations: [
        PureSolTechnologiesComponent,
        PurifinityComponent,
        AppComponent
    ],
    bootstrap: [AppComponent]
})
export class AppModule { }